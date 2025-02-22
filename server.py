import asyncio
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import pytz, time, threading
from ib_insync import *
from pymongo import MongoClient
from itertools import count
import math
from bson import ObjectId
from dotenv import load_dotenv
import os

app = FastAPI()
# Load environment variables from .env file into os.environ
load_dotenv()

port = os.getenv("PORT")
ibkr_api = str(os.getenv("IBKR_API"))
mongo_uri = str(os.getenv("MONGO_URI"))

print("mongo_uri: ", port)
print("ibkr_api: ", ibkr_api)
print("mongo_uri: ", mongo_uri)


# -----------------------------
# MongoDB Setup
# -----------------------------
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["trade_activity"]
trades_collection = mongo_db["trades"]

# -----------------------------
# Global Dictionary to Store Cancellation Events
# -----------------------------
# We'll map a cancel key (string) to a threading.Event.
cancel_events = {}

ib = IB()

ib_order = IB()

ib_order1 = IB()

# -----------------------------
# Global Client ID Counter for IB Connections (if needed)
# -----------------------------
clientId_counter = count(1)

# -----------------------------
# IBKR Connection Helper (with event loop and unique clientId)
# -----------------------------
def connect_to_ibkr(ib_instance: IB):
    try:
        # Ensure current thread has an event loop.
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if not ib_instance.isConnected():
            cid = next(clientId_counter)
            p = ib_instance.connect(ibkr_api, port, clientId=1)
        print("Connected to IBKR at", p)
        print("Connected to IBKR at", ib_instance.reqCurrentTime())

    except Exception as e:
        print(f"Error connecting to IBKR: {e}")
        raise

def ensure_connected(ib_instance: IB):
    if not ib_instance.isConnected():
        connect_to_ibkr(ib_instance)

# -----------------------------
# Data Models for Endpoints
# -----------------------------
class OrderDetails(BaseModel):
    symbol: str
    action: str
    quantity: int
    entry_time: datetime  # ISO-formatted string expected
    exit_time: datetime   # ISO-formatted string expected
    stop_loss_ticks: int

class CancelOrderRequest(BaseModel):
    mongo_id: str  # MongoDB record ID for the pending order

# -----------------------------
# Helper: Wait Until Target Time with Cancellation Support
# -----------------------------
def wait_until(target_time: datetime, cancel_event: threading.Event) -> bool:
    print("Waiting until:", target_time)
    while datetime.now(pytz.utc) < target_time:
        # print("Time Now:", datetime.now(pytz.utc))
        if cancel_event.is_set():
            return False
        time.sleep(1)
    return True


def ensure_event_loop():
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

def Market_data(contract1, symbol):
    # Fetch market price to calculate stop loss price
    market_data = ib.reqMktData(contract1)
    print("Market data:", market_data)
    # while not market_data.last: # Wait a bit for market data to update
    ib.sleep(3)
    current_price = market_data.last
    print(f"Current price of {symbol}: {current_price}")
    return current_price


# -----------------------------
# Main Order Logic Function Using time.sleep and Cancellation Events
# -----------------------------
def place_order(symbol, action, quantity, entry_time, exit_time, stop_loss_ticks):
    
    print('ib place order:', ib)
    cancel_event = threading.Event()
    try:
        ensure_connected(ib)
        ensure_event_loop() 

        print(f"Order received. Entry time: {entry_time}, Exit time: {exit_time}")

        entry_time_utc = entry_time.astimezone(pytz.utc)
        exit_time_utc = exit_time.astimezone(pytz.utc)
        print(f"Entry time (UTC): {entry_time_utc}, Exit time (UTC): {exit_time_utc}")

        # Create an initial trade record (status "Waiting for Entry")
        pending_record = {
            "entryOrderId": None,
            "exitOrderId": None,
            "stopLossOrder": {},
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "entry_time": entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            "exit_time": exit_time.strftime('%Y-%m-%d %H:%M:%S'),
            "stop_loss_ticks": stop_loss_ticks,
            "entry_price": None,
            "stop_loss_order_placed": False,
            "StopLossExecuted": False, 
            "stop_loss_price": None,
            "exit_price": None,
            "status": "Waiting for Entry"
        }
        # Insert pending record; obtain its _id as a string (our cancel key).
        result = trades_collection.insert_one(pending_record)
        mongo_id = str(result.inserted_id)
        print("Stored pending trade record with mongo_id:", mongo_id)

        # Save cancellation event in global dict, using the mongo_id as key.
        cancel_events[mongo_id] = cancel_event

        # -----------------------------
        # Wait until Entry Time
        # -----------------------------
        if not wait_until(entry_time_utc, cancel_event):
            print("Order cancelled before entry time.")
            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                         {"$set": {"status": "Cancelled"}})
            return

        # -----------------------------
        # Place Entry Order at Entry Time
        # -----------------------------
        contract1 = Stock(symbol, 'SMART', 'USD')
        ib.qualifyContracts(contract1)

        entry_order = MarketOrder(action, quantity)
        trade = ib.placeOrder(contract1, entry_order)
        print(f"Placed entry order: {action} {quantity} {symbol}")
        
        while trade.orderStatus.orderId == 0:
            if cancel_event.is_set():
                print("Order cancelled while waiting for entry order acknowledgement.")
                return
            ib.sleep(1)
        parent_order_id = trade.orderStatus.orderId

        current_price = Market_data(contract1, symbol)

        print(f"Entry order acknowledged with orderId: {parent_order_id}")
        trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                     {"$set": {"entryOrderId": parent_order_id, "entry_price": current_price,
                                               "status": "Entry Placed"}})

        # -----------------------------
        # Place Stop-Loss Order (in the same thread here)
        # -----------------------------

        if action.upper() == "BUY":
            stop_loss_price = current_price - (stop_loss_ticks * 0.01)
        else:
            stop_loss_price = current_price + (stop_loss_ticks * 0.01)
        
        print(f"Calculated stop-loss price: {stop_loss_price}")
        stop_order = StopOrder('SELL' if action.upper() == "BUY" else 'BUY', quantity, stop_loss_price)
        stop_order_placed = ib.placeOrder(contract1, stop_order)
        
        # Serialize stop-loss order (extract relevant fields)
        stop_order_data = {
            "orderId": stop_order.orderId,
            "clientId": stop_order.clientId,
            "action": stop_order.action,
            "totalQuantity": stop_order.totalQuantity,
            "auxPrice": stop_loss_price
        }

        print(f"Stop-loss order placed for: {stop_order_placed}")
        if not math.isnan(stop_loss_price):
            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                    {"$set": {"stop_loss_order_placed": True,
                                                "stop_loss_price": stop_loss_price, "stopLossOrder": stop_order_data }})

        # -----------------------------
        # Background Thread: Place Exit Order at Exit Time (with condition check)
        # -----------------------------
        def exit_thread():
            try:
                # ensure_connected(ib)
                ensure_event_loop() 
                print('ib exit:', ib)
                now = datetime.now(pytz.utc)
                diff = (exit_time_utc - now).total_seconds()
                print(f"Exit thread waiting for {diff} seconds until exit time")
                if not wait_until(exit_time_utc, cancel_event):
                    print("Exit thread cancelled before exit time.")
                    return
                if cancel_event.is_set():
                    print("Exit thread detected cancellation.")
                    return


                # Retrieve stop-loss price from DB
                trade_record = trades_collection.find_one({"_id": ObjectId(mongo_id)})
                if trade_record and trade_record.get("stop_loss_price") is not None:
                    
                    stop_loss_order_data = trade_record.get("stopLossOrder")

                    # Deserialize the StopOrder from the stored dictionary
                    stop_loss_order = StopOrder(
                        orderId=stop_loss_order_data.get("orderId"),
                        clientId=stop_loss_order_data.get("clientId"),
                        action=stop_loss_order_data.get("action"),
                        totalQuantity=stop_loss_order_data.get("totalQuantity"),
                        stopPrice=trade_record.get("stop_loss_price")
                    )
                    print('stop_loss_order data: ', stop_loss_order)

                    # Checking if stop order is stil open - 
                    # if yes than it means stop loss is not executed yet
                    open_orders = ib.openOrders()
                    for open_order in open_orders:
                        if open_order.orderId == stop_loss_order_data.get("orderId"):
                            print("StopOrder is still Open with ID: ", open_order.orderId, "and",
                                stop_loss_order_data.get("orderId"))
                            stop_order_open = True
                            break
                        
                    # Exit the trade if stoploss trade is not executed till exit time.
                    # else if stop loss is executed than execute exit trade
                    if stop_order_open and stop_order_open == True:
                        print("Exit condition met for trade: Canceling StopLoss trade, and executing exit ")
                        ib.cancelOrder(stop_loss_order)   
                        ib.sleep(2)
                    else:
                        print("Exit condition not met for trade. Not executing exit.")
                        if not math.isnan(stop_loss_price):
                            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                            {"$set": {"exit_price": stop_loss_price,
                                                    "status": "Stop Executed", "StopLossExecuted": True}})
                        return
                else:
                    print("Data for the trade is not in database. Cannot determine exit condition.")
                    return

                # Place exit order if condition met.
                exit_order = MarketOrder('SELL' if action.upper() == "BUY" else 'BUY', quantity)
                exit_trade = ib.placeOrder(contract1, exit_order)
                print(f"Placed exit order for {symbol}")

                # Fetch market price to calculate stop loss price
                exit_price = Market_data(contract1, symbol)

                while exit_trade.orderStatus.orderId == 0:
                    if cancel_event.is_set():
                        print("Exit thread cancelled while waiting for acknowledgement.")
                        return
                    ib.sleep(1)
                
                exit_order_id = exit_trade.orderStatus.orderId

                print(f"Exit order acknowledged with orderId: {exit_order_id}")
                trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                             {"$set": {"exitOrderId": exit_order_id,
                                                       "exit_price": exit_price,
                                                       "status": "Exit Executed"}})
            except Exception as e:
                print("Error in exit_thread:", e)
            finally:
                print("exit_thread ended")
                # ib.disconnect()

        threading.Thread(target=exit_thread).start()

    except Exception as e:
        print("Error placing order:", e)
    finally:
        # ib.disconnect()
        print("Main IBKR.")

# -----------------------------
# Cancel Order Function (Based on mongo_id)
# -----------------------------
def cancel_order_by_mongo_id(mongo_id: str):
    # Look up the record by mongo_id
    record = trades_collection.find_one({"_id": ObjectId(mongo_id)})
    if not record:
        print("No record found with given mongo_id.")
        return
    # If the trade is still pending (i.e. entryOrderId is None), use the cancellation event
    if record.get("entryOrderId") is None or record.get("exitOrderId") is None:
        if mongo_id in cancel_events:
            cancel_events[mongo_id].set()
            del cancel_events[mongo_id]
            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                         {"$set": {"status": "Cancelled"}})
            print(f"Pending trade with mongo_id {mongo_id} cancelled.")
        
        # Exit order is not placed yet and user want to close the position right away
        if record.get("entryOrderId") is not None and record.get("exitOrderId") is None:
            print("Sell the related position, Exit is canceled")
            
            #Checking If stop loss has executed, if yes than don't sell position again
            def exit_and_close_before():
                
                ensure_event_loop()

                if record and record.get("stop_loss_price") is not None:
                    try:
                        print(f"Stop-loss price from record: {record.get("stop_loss_price")}")
                        stop_loss_order_data = record.get("stopLossOrder")

                        # Deserialize the StopOrder from the stored dictionary
                        stop_loss_order = StopOrder(
                            orderId=stop_loss_order_data.get("orderId"),
                            clientId=stop_loss_order_data.get("clientId"),
                            action=stop_loss_order_data.get("action"),
                            totalQuantity=stop_loss_order_data.get("totalQuantity"),
                            stopPrice=record.get("stop_loss_price")
                        )
                        print('stop_loss_order data: ', stop_loss_order)
                        
                        stop_order_open = False
                        # Checking if stop order is stil open - 
                        # if yes than it means stop loss is not executed yet
                        open_orders = ib.openOrders()
                        print(open_orders)
                        for open_order in open_orders:
                            print("open order: ",open_order.orderId) 
                            print("stop_loss_order_id", stop_loss_order_data.get("orderId")) 

                            if open_order.orderId == stop_loss_order_data.get("orderId"):
                                print("StopOrder is still Open with ID: ", open_order.orderId, "and",
                                    stop_loss_order_data.get("orderId"))
                                stop_order_open = True
                                break
                            
                        # Exit the trade if stoploss trade is not executed till exit time.
                        # else if stop loss is executed than execute exit trade
                        if stop_order_open and stop_order_open == True:
                            print("Exit condition met for trade: Canceling StopLoss trade, and executing exit ")
                            ib.cancelOrder(stop_loss_order)  
                            contract1 = Stock(symbol=record.get("symbol"), exchange='SMART', currency='USD')
                            exit_order = MarketOrder('SELL' if record.get("action").upper() == "BUY" else 'BUY', record.get("quantity"))
                            exit_trade = ib.placeOrder(contract1, exit_order)
                            print(f"Placed exit order for {record.get("symbol")}")

                            # p = await ib_order1.connect(ibkr_api, port, clientId=2)
                            # print("Connected to IBKR at", p)

                            # # Fetch market price to calculate stop loss price
                            # market_data = ib_order1.reqMktData(contract1)
                            # print("Market data:", market_data)
                            # # while not market_data.last: # Wait a bit for market data to update
                            # ib_order1.sleep(3)
                            # current_price = market_data.last

                            time_now = datetime.now()
                            exit_time = time_now.strftime('%Y-%m-%d %H:%M:%S')
                            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                                        {"$set": {"exit_time": exit_time,
                                                                "status": "Manual Exit Executed"}})

                            while exit_trade.orderStatus.orderId == 0:
                                ib.sleep(1)
                            
                            exit_order_id = exit_trade.orderStatus.orderId

                            print(f"Exit order acknowledged with orderId: {exit_order_id}")
                            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                                        {"$set": {"exitOrderId": exit_order_id}})
                        else:
                            print("Exit condition not met for trade. Not executing exit.")
                            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                                {"$set": {"exit_price": record.get("stop_loss_price"),
                                                        "status": "Stop Already Executed"}})
                            return
                        
                    except Exception as e:
                        print("Error in Cancel Order Endpoint: ", e)
            
                else:
                    print("Data for the trade is not in database. Cannot determine exit condition.")
                    return 

            threading.Thread(target=exit_and_close_before).start()
        
        else:
            print("No active cancellation event for this pending trade.")
    else:
        # If the trade already has an entryOrderId, try to cancel using that key.
        entryOrderId = record.get("entryOrderId")
        if entryOrderId in cancel_events:
            cancel_events[entryOrderId].set()
            del cancel_events[entryOrderId]
            trades_collection.update_one({"_id": ObjectId(mongo_id)},
                                         {"$set": {"status": "Cancelled"}})
            print(f"Trade with entryOrderId {entryOrderId} cancelled (if not already executed).")
        else:
            print("No active cancellation event found. It may have been executed already.")

# -----------------------------
# Refresh Trade Activity (from MongoDB)
# -----------------------------
@app.get("/orders")
async def get_orders():
    try:
        orders = list(trades_collection.find({"entryOrderId": {"$exists": True}}))
        p = await ib_order.connectAsync(ibkr_api, port, clientId=3)
        print("Connected to IBKR at", p)

        # Example usage
        exec_filter = ExecutionFilter()
        executions = await ib_order.reqExecutionsAsync(exec_filter)

        for order in orders:
            # Extract order IDs
            entry_id = order.get("entryOrderId")
            exit_id = order.get("exitOrderId")
            stop_id = order.get("stopLossOrder", {}).get("orderId") or ""

            # Initialize updated values
            updated_entry_price = None
            updated_exit_price = None
            updated_stop_loss_price = None

            # Loop through executions to find matching orders
            for fill in executions:
                order_id = fill.execution.orderId
                price = fill.execution.price

                if order_id == entry_id:
                    updated_entry_price = price
                elif order_id == exit_id:
                    updated_exit_price = price
                elif order_id == stop_id:
                    updated_stop_loss_price = price

            # Apply updates only if values were found
            if updated_entry_price is not None:
                order["entry_price"] = round(updated_entry_price, 2)
            if updated_exit_price is not None:
                order["exit_price"] = round(updated_exit_price, 2)
            if updated_stop_loss_price is not None:
                order["stop_loss_price"] = round(updated_stop_loss_price, 2)
                order["status"] = "Stop Executed"
                order["StopLossExecuted"] = True

        for order in orders:
            order["_id"] = str(order["_id"])

        return orders
    except Exception as e:
        print("ex: ", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        print("ib_order disconnected")
        ib_order.disconnect()

# -----------------------------
# API Endpoints
# -----------------------------
@app.get("/check")
def read_root():
    return {"Hello": "World"}

@app.post("/place_order")
async def place_order_endpoint(order: OrderDetails, background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(
            place_order,
            order.symbol,
            order.action,
            order.quantity,
            order.entry_time,
            order.exit_time,
            order.stop_loss_ticks,
        )
        return {"message": "Order processing started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cancel_order")
async def cancel_order_endpoint(cancel_req: CancelOrderRequest):
    try:
        cancel_order_by_mongo_id(cancel_req.mongo_id)
        return {"message": "Order cancelled"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
