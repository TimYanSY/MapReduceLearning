from mrjob.job import MRJob
from mrjob.step import MRStep
# mapper parse input, yield customer id and amount
# reducer take in customer id and sum up amount

class MRCustomerOrders(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_amount,
                reducer=self.reducer_count_amount
            ),
            MRStep(
                mapper=self.mapper_make_amount_key,
                reducer=self.reducer_output_amount
            )
        ]
    def mapper_get_amount(self, key, line):
        (customerID, _itemID, amount) = line.split(',')
        yield customerID, float(amount)

    def reducer_count_amount(self, customerID, amounts):
        yield customerID, sum(amounts)
    # def reducer_totals_by_customer(self, customerID, orders):
    #     yield customerID, sum(orders)

    def mapper_make_amount_key(self, customerID, amount):
        yield '%04.02f'%float(amount), customerID

    def reducer_output_amount(self, amount, customerIDs):
        for customerID in customerIDs:
            yield customerID, amount

if __name__ == '__main__':
    MRCustomerOrders.run()