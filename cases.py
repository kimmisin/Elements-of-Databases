import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatDate(beam.DoFn):
  def process(self, element):
    location_id = element['location_id']
    last_update = str(element['last_update'])
    confirmed = element['confirmed']
    deaths = element['deaths']
    recovered = element['recovered']
    active = element['active']
    rate = element['incident_rate']
    ratio = element['case_fatality_ratio']
    
    split_date = last_update.split('T')
    
    if len(split_date) > 1:
        date = split_date[0]
        time = split_date[1]
        last_update = date + ' ' + time
        
    record = {'location_id': location_id, 'last_update': last_update, 'confirmed': confirmed, 'deaths': deaths, 'recovered': recovered, 'active': active, 'incident_rate': rate, 'case_fatality_ratio': ratio}
    return [record]
           
def run():
     PROJECT_ID = 'lunar-analyzer-302702'
     BUCKET = 'gs://apachebeam-bucket/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM datamart.cases limit 499'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     out_pcoll = query_results | 'Format last_update' >> beam.ParDo(FormatDate())

     out_pcoll | 'Log output' >> WriteToText('output_date.txt')

     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'cases_Beam'
     schema_id = 'location_id:INTEGER,last_update:DATETIME,confirmed:INTEGER,deaths:INTEGER,recovered:INTEGER,active:INTEGER,incident_rate:FLOAT,case_fatality_ratio:FLOAT'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()