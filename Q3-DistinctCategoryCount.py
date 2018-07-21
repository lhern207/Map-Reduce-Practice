"""
 Python script to find the number of distinct product categories in data.csv
 using Map-Reduce framework (mapper, combiner, and reducer functions) with mrjob package
 4/14/17
"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class CategoryCount(MRJob):
# each input lines consists of city, productCategory, price, and paymentMode

    def steps(self):
      return [
         MRStep(mapper=self.mapper,
               combiner=self.combiner,
               reducer=self.reducer),
         MRStep(combiner=self.catCountCombiner,
		        reducer=self.catCountReducer)
      ]

    def mapper(self, _, line):
        # create a key-value pair with key: productCategory and value: 1
        line_cols = line.split(',')
        yield line_cols[1], 1

    def combiner(self, category, counts):
        # consolidates all key-value pairs of mapper function (performed at mapper nodes)
        yield category, 1

    def reducer(self, category, counts):
        # final consolidation of key-value pairs at reducer nodes
        yield category, 1
   
    def catCountCombiner(self, _, category):
       # creates a pair of descriptor key and count(1) per disctinct category
	   yield "Distinct Categories", 1
       
    def catCountReducer(self, key, counts):
       # consolidates the total number of distinct categories
	   yield key, sum(counts)


if __name__ == '__main__':
    CategoryCount.run()
