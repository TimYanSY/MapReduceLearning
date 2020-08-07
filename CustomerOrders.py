from mrjob.job import MRJob
# mapper parse input, yield customer id and amount
# reducer take in customer id and sum up amount

class MRCustomerOrders(MRJob):
    def mapper(self, key, line):
        (customerID, _itemID, amount) = line.split(',')
        yield customerID, float(amount)

    def reducer(self, customerID, amount):
        yield customerID, sum(amount)

if __name__ == '__main__':
    MRCustomerOrders.run()