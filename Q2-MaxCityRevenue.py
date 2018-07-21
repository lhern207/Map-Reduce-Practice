"""
 Python script to find the top 3 cities with the highest amount of revenue in data.csv
 using Map-Reduce framework (mapper, combiner, and reducer functions) with mrjob package
 4/14/17
"""
from mrjob.job import MRJob
from mrjob.step import MRStep


class CityRevenue(MRJob):
# each input lines consists of city, productCategory, price, and paymentMode

    count = 0

    def steps(self):
	   return [
         MRStep(mapper=self.mapper,
               combiner=self.combiner,
               reducer=self.reducer),
         MRStep(reducer=self.maxCityReducer)
      ]

    def mapper(self, _, line):
        # create a key-value pair with key: city and value: price
        line_cols = line.split(',')
        yield line_cols[0], float(line_cols[2])

    def combiner(self, city, counts):
        # consolidates all key-value pairs of mapper function (performed at mapper nodes)
        yield city, sum(counts)

    def reducer(self, city, counts):
        # final consolidation of key-value pairs at reducer nodes
        yield None, ('${:,.2f}'.format(sum(counts)), city)

    def maxCityReducer(self, _, city_counts_pairs):
       # each item of city_count_pairs is (count, city)
	   # create a list of tuples (pairs) sorted in descending revenue order
	   # Select only the first three tuples on the list and assign them to topcities
       topcities = sorted(city_counts_pairs, reverse=True)[:3]
       yield "Top 3 revenue cities", topcities

if __name__ == '__main__':
    CityRevenue.run()
