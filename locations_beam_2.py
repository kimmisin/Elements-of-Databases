import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

# change the abbreviated US state names to their full name
class StandardizeState(beam.DoFn):
  def process(self, element):
    # get individual columns from element
    # get state name in uppercase without leading/trailing whitespace
    l_id = element['id']
    fips = element['fips']
    admin2 = element['admin2']
    city = element['city']
    state = element['state'].strip().upper()
    country = element['country']
    lat = element['latitude']
    longi = element['longitude']
    combine = element['combined_key']

    # state abbreviation dictionary 
    us_states = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'DC': 'District of Columbia',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'MP': 'Northern Mariana Islands',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VI': 'Virgin Islands',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming'
    }
    
    # return record with new state name
    if (state in us_states.keys()):
        state = us_states[state]
        record = {'id': l_id, 'fips': fips, 'admin2': admin2, 'city': city, 'state': state, 'country': country,'latitude': lat,'longitude': longi, 'combined_key': combine}
        return [record]
    # return None for state since US is not a state name
    elif (state == 'US'):
        record = {'id': l_id, 'fips': fips, 'admin2': admin2, 'city': city, 'state': None, 'country': country,'latitude': lat,'longitude': longi, 'combined_key': combine}
        return [record]
    # state did not match a dictionary key, print element for debugging
    else:
        print(element)
        return
        
def run():
     # set up
     PROJECT_ID = 'lunar-analyzer-302702'
     BUCKET = 'gs://apachebeam-bucket/temp'
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # use DirectRunner
     p = beam.Pipeline('DirectRunner', options=opts)

     # BigQuery query with LIMIT clause for less than 500
     sql = 'SELECT * FROM datamart.new_locations WHERE country="US" AND LENGTH(TRIM(state))=2 ORDER BY city, state LIMIT 499'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # create a PCollection from the BigQuery query results
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # feed query_results PCollection to PTransform Pardo(StandardizeState()) to get a PCollection of elements with standarized state names
     state_pcoll = query_results | 'Standardize State Names' >> beam.ParDo(StandardizeState())

     # write the PCollection of standardized state locations to 'output.txt'
     state_pcoll | 'Log output' >> WriteToText('output.txt')

     # setup variables for writing the PCollection of unique locations to a new BigQuery table
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'locations_Beam'
     schema_id = 'id:INTEGER,fips:INTEGER,admin2:STRING,city:STRING,state:STRING,country:STRING,latitude:FLOAT,longitude:FLOAT,combined_key:STRING'

     # write the PCollection of standardized state locations to a new BigQuery table or replace the table if it exists
     state_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET, write_disposition='WRITE_TRUNCATE')
     
     # run the pipeline until finshed
     result = p.run()
     result.wait_until_finish()      

# call run()
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()