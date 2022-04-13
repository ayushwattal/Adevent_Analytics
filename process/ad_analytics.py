from google.cloud.storage import client
import apache_beam as beam
from apache_beam.runners.interactive import interactive_runner
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam import Flatten, Create, ParDo, Map
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.window import FixedWindows
import user_agents
from apache_beam.runners import DataflowRunner

import google.auth
import json
from datetime import datetime, timedelta
import datetime
import numbers

# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=['--streaming'])

# Sets the pipeline mode to streaming, so we can stream the data from PubSub.
options.view_as(pipeline_options.StandardOptions).streaming = True

# Sets the project to the default project in your current Google Cloud environment.
# The project will be used for creating a subscription to the Pub/Sub topic.
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

options.view_as(GoogleCloudOptions).region = 'us-west1'
options.view_as(GoogleCloudOptions).staging_location = 'gs://advents-dataflow-processing/staging'
options.view_as(GoogleCloudOptions).temp_location = 'gs://advents-dataflow-processing/temp'

#options.view_as(pipeline_options.SetupOptions).sdk_location = (
 #   f'/root/apache-beam-custom/packages/beam/sdks/python/dist/apache-beam-{beam.version.__version__}0.tar.gz' 

#Topic to inform about new batch folder(server and client data)
topic_batchdata = "projects/advertising-analytics-grp5/topics/adevents_batch_data_informer"

#Topic for Client stream events
topic_client = "projects/advertising-analytics-grp5/topics/client_events"

#Topic for Server stream events
topic_server = "projects/advertising-analytics-grp5/topics/server_events"


#Starting Beam pipeline
with beam.Pipeline(options=options) as pipeline:

    #Function to Enrich Ad Position Data
    def merging_position_data(client_data, ad_positions_input):
        if(client_data['ad_position'] in ad_positions_input):
            client_data['Location'] = ad_positions_input[client_data['ad_position']]['Location']
            client_data['Size'] = ad_positions_input[client_data['ad_position']]['Size']
        else:
            client_data['Location'] = "Unknown"
            client_data['Size'] = "Unknown"
        return client_data

    #Function to Enrich for Advertiser Data after enriching for Adposition
    def merging_advertiser_data(client_adposition_data, advertiser_input):
        if(client_adposition_data['ad_id'] in advertiser_input):
            client_adposition_data['advertiser'] = advertiser_input[client_adposition_data['ad_id']]['advertiser']
            client_adposition_data['exchange'] = advertiser_input[client_adposition_data['ad_id']]['exchange']
            client_adposition_data['campaign_id'] = advertiser_input[client_adposition_data['ad_id']]['campaign_id']
        else:
            client_adposition_data['advertiser'] = "Unknown"
            client_adposition_data['exchange'] = "Unknown"
            client_adposition_data['campaign_id'] = "Unknown"
        return client_adposition_data

    #Function to Enrich for Publisher Data after enriching for Advertiser
    def merging_page_data(client_adp_adv_data, page_input):
        lookup_key = str(client_adp_adv_data['publisher_id'])+"##"+str(client_adp_adv_data['page_id'])
        if(lookup_key in page_input):
            client_adp_adv_data['publisher'] = page_input[lookup_key]['publisher']
            client_adp_adv_data['article'] = page_input[lookup_key]['article']
        else:
            client_adp_adv_data['publisher'] = "Unknown"
            client_adp_adv_data['article'] = "Unknown"
        return client_adp_adv_data

   
    #Class to validate for Datatype , Null data etc.,    
    class ValidateClientEvents(beam.DoFn):
        """Prints per session information"""
        def process(self, element, window=beam.DoFn.WindowParam):
            element["validity_status"] = ""
            element["invalid_field"] = ""
            if (element['ad_id'] == None or str(element["ad_id"]).isnumeric == False):
                    if element['ad_id'] == None:
                        element["validity_status"] = "Invalid due to null"
                        element["invalid_field"] = "ad_id"
                    else:
                        element["validity_status"] = "Invalid due to Data type"
                        element["invalid_field"] = "ad_id"
            else:
                if element['user_id'] == None:
                    element["validity_status"] = "Invalid due to null"
                    element["invalid_field"] = "user_id"
                else:
                    if (element['publisher_id'] == None or str(element["publisher_id"]).isnumeric == False):
                        if element['publisher_id'] == None:
                            element["validity_status"] = "Invalid due to null"
                            element["invalid_field"] = "publisher_id"
                        else:
                            element["validity_status"] = "Invalid due to Data type"
                            element["invalid_field"] = "publisher_id" 
                    else:
                        if element['page_id'] == None:
                            element["validity_status"] = "Invalid due to null"
                            element["invalid_field"] = "page_id"
                        else:
                            if element['ad_position'] == None:
                                element["validity_status"] = "Invalid due to null"
                                element["invalid_field"] = "ad_position"
                            else:
                                if element['user_agent'] == None:
                                    element["validity_status"] = "Invalid due to null"
                                    element["invalid_field"] = "user_agent"
                                else:
                                    if element['latency'] == None:
                                        element["validity_status"] = "Invalid due to null"
                                        element["invalid_field"] = "latency"
                                    else:
                                        if element['ctimestamp'] == None:
                                            element["validity_status"] = "Invalid due to null"
                                            element["invalid_field"] = "ctimestamp"
                                        else:
                                            if element['clicked'] == None or (element['clicked'] != 'yes' and element['clicked'] != 'no') :
                                                if element['clicked'] == None:
                                                    element["validity_status"] = "Invalid due to null"
                                                    element["invalid_field"] = "clicked"
                                                else:
                                                    element["validity_status"] = "Invalid due to Data type"
                                                    element["invalid_field"] = "clicked"
        
            yield element

    #Reading Enrichment data from Ad_position table in Bigquery
    ad_position_query = (pipeline | "AdPosition BQ" >> beam.io.ReadFromBigQuery(table='advertising-analytics-grp5:Ad_enriching.Ad_position')) 
    ad_position_keyed_data = ( ad_position_query | 'Keyed AdPosition' >> beam.Map(lambda x :(x['ad_position'],x)))

    #Reading Enrichment data from Advertiser table in Bigquery
    advertiser_query = (pipeline | "Advertiser BQ" >> beam.io.ReadFromBigQuery(table='advertising-analytics-grp5:Ad_enriching.Advertiser')) 
    advertiser_keyed_data = (advertiser_query | 'Keyed Advertiser' >> beam.Map(lambda x :(x['ad_id'], x)))

    #Reading Enrichment data from publisher(Page) table in Bigquery
    page_query = (pipeline | "Page BQ" >> beam.io.ReadFromBigQuery(table='advertising-analytics-grp5:Ad_enriching.Page'))    
    page_keyed_data = (page_query| 'Keyed Page' >> beam.Map(lambda x :(str(x['publisher_id'])+"##"+str(x['page_id']), x)))

    
    #Reding Client Ad Events Stream
    data_client = pipeline | "read client stream" >> beam.io.ReadFromPubSub(topic=topic_client)
    windowed_data_client = (data_client | "client stream window" >> beam.WindowInto(beam.window.FixedWindows(30)))
    converted_client_stream = (windowed_data_client |"Convert Client Stream To json" >> beam.Map(json.loads)
                            |"Keyed client stream" >> beam.Map(lambda x :((str(x['ad_id'])+"#"+x['ad_position']+"#"+x['user_id']+"#"+str(x['publisher_id'])+"#"+str(x['page_id'])), x)))

    #Reding Server Ad Events Stream
    data_server = pipeline | "read server strem" >> beam.io.ReadFromPubSub(topic=topic_server)
    windowed_data_server = (data_server | "server stream window" >> beam.WindowInto(beam.window.FixedWindows(30)))
    converted_server_stream = (windowed_data_server |"Convert Server Stream to json" >> beam.Map(json.loads)
                            |"Keyed server stream" >> beam.Map(lambda x :((str(x['ad_id'])+"#"+x['ad_position']+"#"+x['user_id']+"#"+str(x['publisher_id'])+"#"+str(x['page_id'])), x)))

    #Reding Batch Ad Events containg GCS Folder info
    data_batch = pipeline | "read batch topic" >> beam.io.ReadFromPubSub(topic=topic_batchdata)
    windowed_data_batch = (data_batch | "window for batch data" >> beam.WindowInto(beam.window.FixedWindows(30)))
    converted_batch_message = (windowed_data_batch |"Convert Batch Data event To json" >>  beam.Map(json.loads))

    #Creating GCS file path for Client
    batch_client_file_name = (converted_batch_message
                        |"creating client batch file path" >>  beam.Map(lambda x: x['bucket']+"/"+x['folder_path']+"/"+x['client_events_file']))
                
    #Creating GCS file path for Server
    batch_server_file_name = (converted_batch_message
                        |"creating server batch file path" >>  beam.Map(lambda x: x['bucket']+"/"+x['folder_path']+"/"+x['server_events_file']))

    #Reading Client batch data and  from GCS folder
    client_batch_data = (batch_client_file_name  |"reading client batch data" >> beam.io.ReadAllFromText()
                        |"Convert client batch to json" >> beam.Map(json.loads)
                        |"Keyed client batch" >> beam.Map(lambda x :((str(x['ad_id'])+"#"+x['ad_position']+"#"+x['user_id']+"#"+str(x['publisher_id'])+"#"+str(x['page_id'])), x)))


    #Reading Server batch data and  from GCS folder
    server_batch_data = (batch_server_file_name|"reading server batch data" >> beam.io.ReadAllFromText()
                        |"Convert server batch to json" >> beam.Map(json.loads)                                    
                        |"Keyed server batch" >> beam.Map(lambda x :((str(x['ad_id'])+"#"+x['ad_position']+"#"+x['user_id']+"#"+str(x['publisher_id'])+"#"+str(x['page_id'])), x)))


    #Validating Batch Client data against Batch Server Data
    client_server_batch_mapping =  (({'clientTable': client_batch_data, 'serverTable': server_batch_data }) | "Batch Cogrouped" >> beam.CoGroupByKey())
   

    client_server_batch_mapped = (client_server_batch_mapping | "Client Batch Collection" >> beam.Filter(lambda x: (len(x[1]['clientTable'])>0 and len(x[1]['serverTable'])>0)) 
                                | "Get Client Batch" >> beam.Map(lambda x : x[1]['clientTable'][0]))

    #Validating Client Stream data against Server Stream Data
    client_server_stream_mapping =  (({'clientStreamTable': converted_client_stream, 'serverStreamTable': converted_server_stream }) | "Stream Cogrouped" >> beam.CoGroupByKey())
   

    client_server_stream_mapped = (client_server_stream_mapping | "Client Stream Collection" >> beam.Filter(lambda x: (len(x[1]['clientStreamTable'])>0 and len(x[1]['serverStreamTable'])>0)) 
                                | "Get Client Stream" >> beam.Map(lambda x : x[1]['clientStreamTable'][0]))
                                
                                
    #Combine Batch and Stream Data
    client_data_validated = ((client_server_batch_mapped,client_server_stream_mapped) | "Combine Batch Stream " >> beam.Flatten() | "Client Data validation" >> beam.ParDo(ValidateClientEvents()))

    (client_data_validated   | "client validated to bytes" >> beam.Map(lambda x: json.dumps(x).encode("utf-8")) | "client validated to pubsub" >> beam.io.WriteToPubSub(topic="projects/advertising-analytics-grp5/topics/enriched_data_out2"))
  

    valid_client_data =   (client_data_validated  | "Valid Client Data" >> beam.Filter(lambda x:x["validity_status"] == "" and x["invalid_field"] == "" ))
    invalid_client_data = (client_data_validated  | "Invalid Client Data" >> beam.Filter(lambda x:x["validity_status"] != "" or x["invalid_field"] != ""))


    #Enrichment using the Advertiser, publisher, Ad Position Data from Bigquery 
    enriched_data = (valid_client_data |"Enrich Adposition" >> beam.Map(merging_position_data,ad_positions_input=beam.pvalue.AsDict(ad_position_keyed_data)) 
                    | "Enrich Advertiser" >> beam.Map(merging_advertiser_data,advertiser_input=beam.pvalue.AsDict(advertiser_keyed_data))
                    | "Enrich Page" >> beam.Map(merging_page_data,page_input=beam.pvalue.AsDict(page_keyed_data))
                    | "Enrich Browser" >> beam.Map(EnrichBrowser)
                    )

    (enriched_data  | "json enriched bytes" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))) | "enriched to pubsub" >> beam.io.WriteToPubSub(topic="projects/advertising-analytics-grp5/topics/enriched_data_out2")
    
    #Valid data inserting into table: Ad_impressions
    BQ_Valid_Data  = enriched_data  |"Format Client Data for BQ" >> beam.Map(lambda x: { "ad_id": x['ad_id'],"user_id": x['user_id'],"publisher_id": x['publisher_id'],
                                            "page_id": x['page_id'],"ad_position": x['ad_position'],"latency": x['latency'],"timestamp": x['ctimestamp'],"clicked": x['clicked'],
                                            "Location":x['Location'],"Size":x['Size'],"advertiser":x['advertiser'],"exchange":x['exchange'],"campaign_id":x['campaign_id'],"publisher":x['publisher'],
                                            "article":x['article'],"Browser":x['Browser'],"Browser_version":x['Browser_version'],"Client_OS":x["Client_OS"],"Client_OS_Ver":x['Client_OS_Ver'],
                                            "Client_Device":x['Client_Device'],"Client_Device_Brand":x['Client_Device_Brand'],"Client_Device_Model":x['Client_Device_Model']})

    BQ_insert_valid_data = (BQ_Valid_Data | "Valid Data to BigQuery" >> beam.io.WriteToBigQuery("advertising-analytics-grp5:Ad_analytics.Ad_impressions", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


    #Invalid data inserting into table: Ad_impressions_Invalid

    BQ_InValid_Data = invalid_client_data |"Format Invalid Client Data for BQ" >> beam.Map(lambda x: { "ad_id": x['ad_id'],"user_id": x['user_id'],"publisher_id": x['publisher_id'],
                                                                             "page_id": x['page_id'],"ad_position": x['ad_position'],"user_agent": x['user_agent'],
                                                                             "latency": x['latency'],"timestamp": x['ctimestamp'],"clicked": x['clicked'],"validity_status": x['validity_status'],"invalid_field": x['invalid_field']})

    BQ_insert_invalid_data = (BQ_InValid_Data | "Invalid Client Data to BQ" >> beam.io.WriteToBigQuery("advertising-analytics-grp5:Ad_analytics.Ad_impressions_Invalid", write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


    #Informing to pub/sub that a batch folder is processed. This will trigger cloud function to cleanup the Data on GCS
    batch_folder_mark_success = (converted_batch_message
                        | "completed batch folder" >> beam.Map(lambda x: ("foldername", (x['bucket']+"/"+x['folder_path']) ))
                        | "complete batch to json" >> beam.Map(lambda x: json.dumps(x).encode("utf-8")) | "clean gcs to pubsub" >> beam.io.WriteToPubSub(topic="projects/advertising-analytics-grp5/topics/cleanup_gcs"))

    DataflowRunner().run_pipeline(pipeline, options=options)

    