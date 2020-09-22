import unittest
import pandas as pd 
class AggregationTest(unittest.TestCase):

    def test_decimals(self):

        lst = [['source a', 1232456789, 1232456000, 1232456700, 1234 ],
               ['source a', 1232456789, 1232456000, 1232456700, 1200 ]]        

        df = pd.DataFrame(lst, columns = ['Source', 'total', 'ava', 'exp', 'lat'])

        gr = df.groupby(['Source']).agg({
            'total': 'sum',
            'ava': 'sum',
            'exp': 'sum',
            'lat': 'mean',
        }).reset_index()
        gr['ava_prop'] = gr['ava'].divide(gr['total'])
        gr['exp_prop'] = gr['exp'].divide(gr['total'])
        print(gr.dtypes)
        print(gr.head())


if __name__ == "__main__":
    unittest.main()