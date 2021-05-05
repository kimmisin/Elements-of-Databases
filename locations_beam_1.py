import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

# setup the element for GroupByKey
class FormatForGroupBy(beam.DoFn):
  def process(self, element):
    # get unique location identifers
    lat = element['latitude']
    longi = element['longitude']
    
    # create single unique identifer
    new_key = str(lat) + '-' + str(longi)
    
    # create tuple of the unique identifer and its associated row/element
    lat_tuple = (new_key, element)
    return [lat_tuple]

# remove the duplicate locations via selection criteria
class RemoveDup(beam.DoFn):
  def process(self, element):
    # access element and cast to list
    key, loc_obj = element
    loc_list = list(loc_obj)

    # set up selection variables
    has_key = False
    key_length = 0
    has_fips = False
    fips_num = 0
    country_length = 0
    county_length = 0
    state_length = 0
    keep_idx = 0

    # iterate through each row/element for each key
    for i in range(len(loc_list)):
        combinedKey = loc_list[i].get('combined_key', 0)
        fips = loc_list[i].get('fips', 0)
        # has no comnined_key and fips
        if (combinedKey == None and fips == None and has_key == False and has_fips == False):
            county = loc_list[i].get('county', 0)
            state = loc_list[i].get('state', 0)
            country = loc_list[i].get('country', 0)
            # null state and county
            if (state == None and county == 0):
                # keep the shorter country name
                if len(country) < country_length:
                    country_length = len(country)
                    keep_idx = i
            # non-null state and null county
            elif (state != None and county == 0):
                # keep the longer state name
                if len(state) > state_length:
                    state_length = len(state)
                    keep_idx = i
            # non-null state and county
            elif (state != None and county != 0):
                # keep the longer county name
                if len(county) > county_length:
                    county_length = len(county)
                    keep_idx = i
        # has combined_key but no fips
        elif (combinedKey != None and fips == None and has_fips == False):
            has_key = True
            # keep longer combined_key
            if (len(combinedKey) > key_length):
                key_length = len(combinedKey)
                keep_idx = i
        # has combined_key and fips
        elif (combinedKey != None and fips != None):
            has_key = True
            has_fips = True
            # keep larger fips
            if (fips > fips_num):
                fips_num = fips
                keep_idx = i

    return [loc_list[keep_idx]]
           
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
     sql = 'SELECT * FROM datamart.locations AS l WHERE latitude IN (SELECT latitude FROM datamart.locations AS lat WHERE l.latitude=lat.latitude AND l.id!=lat.id) AND longitude IN (SELECT longitude FROM datamart.locations AS long WHERE l.longitude=long.longitude AND l.id!=long.id) AND latitude!=0 AND longitude!=0 ORDER BY latitude, longitude LIMIT 499'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # create a PCollection from the BigQuery query results
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # feed query_results PCollection to PTransform Pardo(FormatForGroupBy()) to get a PCollection of elements and its unqiue identifer
     loc_pcoll = query_results | 'Setup for GroupByKey' >> beam.ParDo(FormatForGroupBy())

     # feed PCollection of unique identifers and its elements to PTransform (GroupByKey) to get a PCollection of grouped locations/elements via identifer
     loc_grouped_pcoll = loc_pcoll | 'GroupByKey' >> beam.GroupByKey()

     # feed PCollection of grouped locations via identifer to PTranform ParDo(RemoveDup()) to get a PCollection of unique locations
     unique_loc_pcoll = loc_grouped_pcoll | 'Remove duplicates' >> beam.ParDo(RemoveDup())

     # write the PCollection of unique locations to 'output.txt'
     unique_loc_pcoll | 'Log output' >> WriteToText('output.txt')

     # setup variables for writing the PCollection of unique locations to a new BigQuery table
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'locations_Beam'
     schema_id = 'id:INTEGER,fips:INTEGER,admin2:STRING,city:STRING,state:STRING,country:STRING,latitude:FLOAT,longitude:FLOAT,combined_key:STRING'

     # write the PCollection of unique locations to a new BigQuery table or replace the table if exists
     unique_loc_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET, write_disposition='WRITE_TRUNCATE')
    
     # run the pipeline until finshed
     result = p.run()
     result.wait_until_finish()      

# call run()
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()