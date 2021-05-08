import logging, datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

# setup elements for GroupByKey based on latitude and longitude
class FormatForGroupBy(beam.DoFn):
    def process(self, element):
        # get unique location identifers
        l_id = element['id']
        lat = element['latitude']
        longi = element['longitude']
        country = element['country'].strip().upper()
        
        # latitude and longitude are both null/none: return id and element (will not be grouped by GroupByKey)
        if (lat == None or longi == None):
            ungrouped_tuple = (l_id, element)
            return [ungrouped_tuple]
        # latitude and longitude aren't both 0: set up for GroupByKey and RemoveDup()
        elif (lat != 0 or longi != 0): 
            # create single unique identifer
            new_key = str(lat) + '-' + str(longi)
            # create tuple of the unique identifer and its associated row/element
            lat_tuple = (new_key, element)
            return [lat_tuple]
        # latitude and longitude are both 0: return id and element (will not be grouped by GroupByKey)
        else:
            ungrouped_tuple = (l_id, element)
            return [ungrouped_tuple]

# remove the duplicate latitude/longitude locations via selection criteria
class RemoveDup(beam.DoFn):
    def process(self, element):
        # access element and cast to list
        key, loc_obj = element
        loc_list = list(loc_obj)

        # More than one element in list: run through selection criteria
        if (len(loc_list) > 1):
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
                combinedKey = loc_list[i].get('combined_key')
                fips = loc_list[i].get('fips')
                # has no comnined_key and fips
                if (combinedKey == None and fips == None and has_key == False and has_fips == False):
                    county = loc_list[i].get('county', 0)
                    state = loc_list[i].get('state')
                    country = loc_list[i].get('country')
                    # null state and county
                    if (state == None and county == 0):
                        # keep the shorter country name
                        if len(country.strip()) < country_length:
                            country_length = len(country.strip())
                            keep_idx = i
                    # non-null state and null county
                    elif (state != None and county == 0):
                        # keep the longer state name
                        if len(state.strip()) > state_length:
                            state_length = len(state.strip())
                            keep_idx = i
                    # non-null state and county
                    elif (state != None and county != 0):
                        # keep the longer county name
                        if len(county.strip()) > county_length:
                            county_length = len(county.strip())
                            keep_idx = i
                # has combined_key but no fips
                elif (combinedKey != None and fips == None and has_fips == False):
                    has_key = True
                    # keep longer combined_key
                    if (len(combinedKey.strip()) > key_length):
                        key_length = len(combinedKey.strip())
                        keep_idx = i
                # has combined_key and fips
                elif (combinedKey != None and fips != None):
                    has_key = True
                    has_fips = True
                    # keep larger fips
                    if (fips > fips_num):
                        fips_num = fips
                        keep_idx = i
            # return the element that fits our selection criteria
            return [loc_list[keep_idx]]
        # 1 element in the list: just return the single element
        else:
            return [loc_list[0]]

# change the abbreviated US state names to their full name
class StandardizeState(beam.DoFn):
    def process(self, element):
        # get individual columns from element
        l_id = element['id']
        fips = element['fips']
        admin2 = element['admin2']
        city = element['city']
        state = element['state']
        lat = element['latitude']
        longi = element['longitude']
        combine = element['combined_key']
        
        # identify if element is in the US
        country = element['country'].strip().upper()
        if (country == 'US'):
            # get state name in uppercase without leading/trailing whitespace
            state = element['state'].strip().upper()

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
            # state did not match a dictionary key: it is not an abbreviated state name
            else:
                state = element['state'].strip()
                record = {'id': l_id, 'fips': fips, 'admin2': admin2, 'city': city, 'state': state, 'country': country,'latitude': lat,'longitude': longi, 'combined_key': combine}
                return [record]
        # element is not in the US, return as is
        else:
            country = element['country']
            record = {'id': l_id, 'fips': fips, 'admin2': admin2, 'city': city, 'state': state, 'country': country,'latitude': lat,'longitude': longi, 'combined_key': combine}
            return [record]

           
def run():
    # set up
    PROJECT_ID = 'lunar-analyzer-302702'
    BUCKET = 'gs://apachebeam-bucket/temp'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='locations',
    temp_location=BUCKET + '/temp',
    region='us-central1')
    
    # use Dataflow for the pipeline
    p = beam.pipeline.Pipeline(options=options)
    
    # BigQuery query 
    sql = 'SELECT * FROM datamart.locations'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    # create a PCollection from the BigQuery query results
    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

    # feed query_results PCollection to PTransform Pardo(FormatForGroupBy()) to get a PCollection of elements and its unqiue identifer
    loc_pcoll = query_results | 'Setup for GroupByKey' >> beam.ParDo(FormatForGroupBy())

    # feed PCollection of unique identifers and its elements to PTransform (GroupByKey) to get a PCollection of grouped locations/elements via identifer
    loc_grouped_pcoll = loc_pcoll | 'GroupByKey' >> beam.GroupByKey()

    # feed PCollection of grouped locations via identifer to PTranform ParDo(RemoveDup()) to get a PCollection of unique locations
    unique_loc_pcoll = loc_grouped_pcoll | 'Remove duplicates' >> beam.ParDo(RemoveDup())

    # write the PCollection of unique locations to 'locations_unique_output.txt'
    unique_loc_pcoll | 'Log Unique Locations' >> WriteToText(DIR_PATH + 'locations_unique_output.txt')

    # feed PCollection of unique locations to PTransform Pardo(StandardizeState()) to get a PCollection of elements with standarized state names
    state_pcoll = unique_loc_pcoll | 'Standardize State Names' >> beam.ParDo(StandardizeState())

    # write the PCollection of unique and standarized state name locations to 'locations_unique_standardize_output.txt'
    state_pcoll | 'Log Unique and Standardized State Name Locations' >> WriteToText(DIR_PATH + 'locations_unique_standardize_output.txt')

    # setup variables for writing the PCollection of unique locations to a new BigQuery table
    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'locations_Dataflow'
    schema_id = 'id:INTEGER,fips:INTEGER,admin2:STRING,city:STRING,state:STRING,country:STRING,latitude:FLOAT,longitude:FLOAT,combined_key:STRING'

    # write the PCollection of unique and standarized state name locations to a new BigQuery table or replace the table if exists
    state_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET, write_disposition='WRITE_TRUNCATE')
    
    # run the pipeline until finshed
    result = p.run()
    result.wait_until_finish()      

# call run()
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()