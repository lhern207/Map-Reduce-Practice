"""
 Python script to find the payment mode with the highest amount of revenue from data.csv
 using Map-Reduce framework (mapper, combiner, and reducer functions) with mrjob package
 4/14/17
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class ModeRevenue(MRJob):
# each input lines consists of city, productCategory, price, and paymentMode

    # Initialize the count value
    count = 0

    def steps(self):
      return [
         MRStep(mapper=self.mapper,
               combiner=self.combiner,
               reducer=self.reducer),
         MRStep(reducer=self.maxModeReducer)
      ]

    def mapper(self, _, line):
        # create a key-value pair with key: paymentMode and value: price
        line_cols = line.split(',')
        yield line_cols[3], float(line_cols[2])

    def combiner(self, mode, counts):
        # consolidates all key-value pairs of mapper function (performed at mapper nodes)
        yield mode, sum(counts)

    def reducer(self, mode, counts):
        # final consolidation of key-value pairs at reducer nodes
        self.count += 1
        if self.count <= 5:
          yield None, ('${:,.2f}'.format(sum(counts)), mode)

    def maxModeReducer(self, _, mode_count_pairs):
       # each item of mode_count_pairs is (count, mode),
       # so yielding one results in key=counts, value=mode
       yield max(mode_count_pairs)

if __name__ == '__main__':
    ModeRevenue.run()
