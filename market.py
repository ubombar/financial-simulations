from typing import Any, Callable, List, Tuple
import uuid
import heapq
import random
import datetime

class Transaction():
    def __init__(self, mid: uuid.UUID, oid: uuid.UUID, recv: float, asset1: str, send: float, asset2: str) -> None:
        self.id = uuid.uuid1()
        self.mid = mid
        self.oid = oid
        self.asset1 = asset1
        self.asset2 = asset2
        self.recv = recv
        self.send = send
        self.datetime = datetime.datetime.now()

    @property
    def rate(self):
        # This will only work on seller's transactions!
        return self.recv / self.send

    def __str__(self) -> str:
        return "rate: {:0.3f}; {:0.2f} {} for {:0.2f} {} @ {}".format(self.rate, self.recv, self.asset1, self.send, self.asset2, self.datetime)

    def __repr__(self) -> str:
        return str(self)

class Offer():
    def __init__(self, mid:uuid.UUID, vol: float, r: float, handle: Callable[[Transaction], Any]) -> None:
        self.mid = mid
        self.id = uuid.uuid1()
        self.r = r 
        self.vol = vol
        self.handle = handle
    
    def __str__(self) -> str:
        return "(r: {1:0.3f} vol: {0:0.2f})".format(self.vol, self.r)

    def __lt__(self, other):
        return (self.r < other.r)

    def __repr__(self) -> str:
        return str(self)

class Market():
    def __init__(self, asset1: str, asset2: str) -> None:
        self.id = uuid.uuid1()

        self.asset1 = asset1 
        self.asset2 = asset2 

        self.s: List[Offer] = []
        self.d: List[Offer] = [] 

        self.transactions: List[Transaction] = []
    
    @property
    def name(self):
        return "{}/{}".format(self.asset1, self.asset2)
    
    def offer(self, op: str, vol: float, r: float, handle: Callable[[Transaction], Any]) -> uuid.UUID:
        offer = Offer(self.id, vol, r, handle)

        if op in 'sell':
            heapq.heappush(self.s, offer)
        else:
            heapq.heappush(self.d, offer)

        ps = 0
        pd = len(self.d) - 1

        while ps < len(self.s) and pd >= 0:
            seller = self.s[ps]
            buyer = self.d[pd]

            (tseller, tbuyer, status) = self.match(seller, buyer)

            if status == None: break # no transaction can occur!

            seller.vol -= tseller.send # update the balance
            buyer.vol -= tbuyer.send # update the balance

            self.transactions.append(tseller) # only add sellers transaction

            # surplus supply, delete demand
            # equal trade, delete demand and supply
            # shortage supply, delete supply
            if status in {0, 1}: 
                self.d.remove(buyer)
                pd -= 1

            if status in {1, 2}:
                self.s.remove(seller)
                ps += 1

        return offer.id

    def match(self, seller: Offer, buyer: Offer) -> Tuple[Transaction, Transaction, int]:
        if seller.r > buyer.r: return (None, None, None)

        state = 0 # 0: surplus, 1: equal, 2: shortage

        rhat = (seller.r + buyer.r) / 2 

        to_buyer1 = buyer.vol / rhat # buyer recieves apples
        to_seller1 = buyer.vol # seller get dollars

        to_seller2 = seller.vol * rhat
        to_buyer2 = seller.vol

        if to_buyer1 < to_buyer2: # Surplus supply
            state = 0
            tr1 = Transaction(self.id, seller.id, to_seller1, self.asset2, to_buyer1, self.asset1)
            tr2 = Transaction(self.id, buyer.id, to_buyer1, self.asset1, to_seller1, self.asset2)
        elif to_buyer1 == to_buyer2: # Efficient trade
            state = 1
            tr1 = Transaction(self.id, seller.id, to_seller1, self.asset2, to_buyer1, self.asset1)
            tr2 = Transaction(self.id, buyer.id, to_buyer1, self.asset1, to_seller1, self.asset2)
        else: # Shortage on supply 
            state = 2
            tr1 = Transaction(self.id, seller.id, to_seller2, self.asset2, to_buyer2, self.asset1)
            tr2 = Transaction(self.id, buyer.id, to_buyer2, self.asset1, to_seller2, self.asset2)

        return (tr1, tr2, state)

    def remove(self, offerid: int) -> bool:
        return False


if __name__ == "__main__":
    m = Market("apple", "dolar")

    random.seed(0)

    for i in range(1000):
        seller = random.random() > 0.5
        r = random.random() * 1 + 10
        vol = random.random() * 100 + 100

        op = 'sell' if seller else 'buy'
        m.offer(op, vol, r, lambda t: None)

    xrate = []
    for t in m.transactions:
        xrate.append(t.recv / t.send)
        print(t)

    # print(xrate)