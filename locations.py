import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatForGroupBy(beam.DoFn):
  def process(self, element):
    l_id = element['id']
    fips = element['fips']
    admin2 = element['admin2']
    city = element['city']
    state = element['state']
    country = element['country']
    lat = element['latitude']
    longi = element['longitude']
    combine = element['combined_key']
    
    new_key = str(lat) + '-' + str(longi)
    row = element
    lat_tuple = (new_key, row)
    
    return [lat_tuple]

class RemoveDup(beam.DoFn):
  def process(self, element):
    key, loc_obj = element
    loc_list = list(loc_obj)

    has_key = False
    key_length = 0
    has_fips = False
    fips_num = 0
    country_length = 0
    county_length = 0
    state_length = 0
    keep_idx = 0

    for i in range(len(loc_list)):
        combinedKey = loc_list[i].get('combined_key', 0)
        fips = loc_list[i].get('fips', 0)

        if (combinedKey != 0 and fips != 0 and has_key == False and has_fips == False):
            county = loc_list[i].get('county', 0)
            state = loc_list[i].get('state', 0)
            country = loc_list[i].get('country', 0)
            
            if (state == None and county == 0):
                if len(country) < country_length:
                    country_length = len(country)
                    keep_idx = i
            elif (state != None and county == 0):
                if len(state) > state_length:
                    state_length = len(state)
                    keep_idx = i
            elif (state != None and county != 0):
                if len(county) > county_length:
                    county_length = len(county)
                    keep_idx = i
        elif (combinedKey != 0 and fips == 0 and has_fips == False):
            has_key = True
            if (key_length < len(combinedKey)):
                key_length = len(combinedKey)
                keep_idx = i
        elif (combinedKey != 0 and fips != 0):
            has_key = True
            has_fips = True
            if (fips > fips_num):
                fips_num = fips
                keep_idx = i

    return [loc_list[keep_idx]]
           
def run():
     PROJECT_ID = 'lunar-analyzer-302702'
     BUCKET = 'gs://apachebeam-bucket/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM datamart.locations AS l WHERE latitude IN (SELECT latitude FROM datamart.locations AS lat WHERE l.latitude=lat.latitude AND l.id!=lat.id) AND longitude IN (SELECT longitude FROM datamart.locations AS long WHERE l.longitude=long.longitude AND l.id!=long.id) AND latitude!=0 AND longitude!=0 ORDER BY latitude, longitude LIMIT 499'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     loc_pcoll = query_results | 'Setup for GroupByKey' >> beam.ParDo(FormatForGroupBy())

     loc_grouped_pcoll = loc_pcoll | 'GroupByKey' >> beam.GroupByKey()

     unique_loc_pcoll = loc_grouped_pcoll | 'Remove duplicates' >> beam.ParDo(RemoveDup())

     unique_loc_pcoll | 'Log output' >> WriteToText('output.txt')

     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'locations_Beam'
     schema_id = 'id:INTEGER,fips:INTEGER,admin2:STRING,city:STRING,state:STRING,country:STRING,latitude:FLOAT,longitude:FLOAT,combined_key:STRING'

     unique_loc_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()