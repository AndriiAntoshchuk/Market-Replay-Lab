from pathlib import Path
import json
import csv 

class OrderBook():

	def __init__(self):
		self.asks = {}
		self.bids = {}

	# Snapshot replaces the full visible book.
	def load_snapshot(self, asks, bids):
		self.asks = {float(price): float(size) for price, size in asks}
		self.bids = {float(price): float(size) for price, size in bids}		
	
	# Best bids = highest prices first.
	def print_bids(self, num):
		for price, size in sorted(self.bids.items(), reverse=True)[:num]:
			print(f"{price}: {size}")

	# This currently prints the selected ask levels in reverse order.
	def print_asks(self, num):
		for price, size in reversed(sorted(self.asks.items())[:num]):
			print(f"{price}: {size}")

	def print_orderbook(self, num):
		print('//////////////////////////')
		self.print_asks(num)
		print('--------------------------')
		self.print_bids(num) 
		print('//////////////////////////')

	def get_best_bid(self):
		return max(self.bids)
	
	def get_best_ask(self):
		return min(self.asks)
	
	def get_best_bid_size(self):
		return self.bids[max(self.bids)]

	def get_best_ask_size(self):
		return self.asks[min(self.asks)]

	def get_mid_price(self):
		return (self.get_best_ask() + self.get_best_bid()) / 2
	
	def get_spread(self):
		return self.get_best_ask() - self.get_best_bid()
	
	# Apply one Coinbase Level 2 update.
	def apply_update(self, side, price, size):
		price = float(price)
		size = float(size)
		if(side == 'buy'):
			if(size == 0):
				if price in self.bids: del self.bids[price]
			else:
				self.bids[price] = size 
		elif(side == 'sell'):
			if(size == 0):
				if price in self.asks: del self.asks[price]
			else:	
				self.asks[price] = size 
		else:
			raise ValueError(f'Unknown side: {side}')
	
	# Apply all changes contained in one l2update message.
	def apply_list_of_updates(self, update):
		for i in update:
			self.apply_update(i[0], float(i[1]), float(i[2]))

def create_new_dir(path):
	Path(path).mkdir(parents=True, exist_ok=True)

# Replay a raw JSONL file, rebuild the order book,
# and collect top-of-book records for each actionable event.
def create_orderbook(filepath):
	with open(filepath, "r", encoding="utf-8") as f:
		event_counter = 0
		orderbook = None
		rows = []
		for line in f:
			line = json.loads(line)
			message = line['message']
			message_type = message['type']

			# Ignore subscription confirmation messages.
			if(message_type == 'subscriptions'):
				continue 

			# Snapshot replaces the full visible book.
			elif message_type == 'snapshot':
				asks = message['asks']
				bids = message['bids']
				orderbook = OrderBook()
				orderbook.load_snapshot(asks, bids)
				event_counter += 1

			# Incremental updates modify or remove individual price levels.
			elif message_type == "l2update":
				if orderbook is None:
					raise RuntimeError("Received l2update before snapshot")
				orderbook.apply_list_of_updates(message['changes'])
				event_counter += 1

			else:
				raise ValueError(f'Unknown message type: {message_type}')
			
			# Extract top-of-book state after each processed event.
			best_bid = orderbook.get_best_bid()
			best_ask = orderbook.get_best_ask()
			best_bid_size = orderbook.get_best_bid_size()
			best_ask_size = orderbook.get_best_ask_size()		
			spread = orderbook.get_spread()
			mid_price = orderbook.get_mid_price()


			record = {
				"event_no": event_counter,
				"ts_exchange": message["time"],
				"ts_local": line["timestamp"],
				"best_bid": best_bid,
				"best_ask": best_ask,
				"best_bid_size": best_bid_size,
				"best_ask_size": best_ask_size,
				"spread": spread,
				"mid_price": mid_price,
				"spread_bps": 10000 * spread / mid_price,
				"top_book_imbalance": (best_bid_size - best_ask_size) / (best_bid_size + best_ask_size)
			}

			if(event_counter % 100 == 0):
				print(f'Event: {event_counter}')

			rows.append(record)
		
	return orderbook, rows

# Write processed top-of-book records to CSV.
def write_to_csv(rows, filename):
	fieldnames = [
		"event_no",
		"ts_exchange",
		"ts_local",
		"best_bid",
		"best_bid_size",
		"best_ask",
		"best_ask_size",
		"spread",
		"mid_price",
		"spread_bps",
		"top_book_imbalance"
	]

	create_new_dir("data/processed")
	with open(f"data/processed/{filename}", "w", encoding="utf-8", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		writer.writerows(rows)

# Example replay run on one sample raw file.
orderbook, rows = create_orderbook("data/raw/2026-04-21/data_23-17-56-23-18-27.jsonl")
filename = "test.csv"
write_to_csv(rows, filename)


