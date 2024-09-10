
import pandas as pd
import numpy as np
import re
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import time
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from google.ads.googleads import util
from google.cloud.exceptions import NotFound

dt = datetime.today()
start = dt - timedelta(days=dt.weekday())
end = start + timedelta(days=6)
start = start.strftime('%Y-%m-%d')
end = end.strftime('%Y-%m-%d')

gbq_creds = service_account.Credentials.from_service_account_file('secrets_bq.json')
bq_client = bigquery.Client(credentials=gbq_creds, project='go0gle-ads')
#the functions that run query, process data, and then dump onto BQ. 
#summary functions return a pandas dataframe which is then passed to the corresponding conversion report function.
#check to see if cost_micros is the right unit, delete function is working, and if the text columns has the right format, check column names

def getCampaignMergeData(googleads_client, client_id):
    ga_service = googleads_client.get_service("GoogleAdsService")

    #search term view
    query = f"""
    SELECT segments.date, customer.descriptive_name, customer.id, campaign.name, campaign.id, campaign.advertising_channel_type, ad_group_ad.labels, ad_group.labels, campaign.labels,
    ad_group.type, ad_group.name, ad_group.id, segments.keyword.info.text, 
    search_term_view.search_term, ad_group_ad.ad.type, ad_group_ad.ad.id, ad_group_ad.ad.expanded_text_ad.headline_part1, 
    ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.expanded_text_ad.description, 
    customer.currency_code, metrics.impressions, 
    metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.all_conversions, metrics.all_conversions_value 
    FROM search_term_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
        """
    tmpcolumns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Campaign_Type', 'Ad_Group_Ad_Labels', 'Ad_Group_Labels', 'Campaign_Labels', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 
                'Search_Keyword', 'Search_Term',  'Ad_Type', 'Ad_ID', 'Headline_1', 
                'Headline_2', 'Headline_3', 'Description', 'Currency', 'Impressions', 'Clicks', 'Cost', 'Conversions', 
                'Conversion_Values', 'All_Conversions', 'All_Conversions_Values']

    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    tmp = pd.DataFrame(columns = tmpcolumns)
    for batch in stream:
        for row in batch.results:
            search_term_view = row.search_term_view
            segments = row.segments
            ad_group = row.ad_group
            ad_group_ad = row.ad_group_ad
            campaign = row.campaign
            customer = row.customer
            metrics = row.metrics
            df_entry = [segments.date, customer.descriptive_name, customer.id, campaign.name, 
                        campaign.id, campaign.advertising_channel_type.name,  ad_group_ad.labels, ad_group.labels, campaign.labels, ad_group.type_.name, ad_group.name, ad_group.id, segments.keyword.info.text, 
    search_term_view.search_term, ad_group_ad.ad.type_.name, ad_group_ad.ad.id, ad_group_ad.ad.expanded_text_ad.headline_part1,
    ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.expanded_text_ad.description, customer.currency_code, 
    metrics.impressions, 
    metrics.clicks, metrics.cost_micros/1000000, metrics.conversions, metrics.conversions_value, metrics.all_conversions, metrics.all_conversions_value 
            ]
            
            tmp = tmp.append(pd.Series(df_entry, index = tmpcolumns), ignore_index = True)

    tmp[['Impressions','Clicks']] = tmp[['Impressions','Clicks']].astype(int)
    tmp[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost', 'Ad_ID']] = tmp[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost', 'Ad_ID']].astype(float)




    query = f"""
    SELECT segments.date, customer.descriptive_name, customer.id, campaign.name, campaign.id, campaign.advertising_channel_type, ad_group.labels, campaign.labels, ad_group.type, ad_group.name, ad_group.id, segments.keyword.info.text, search_term_view.search_term, ad_group_ad.ad.type, ad_group_ad.ad.id,
    ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.expanded_text_ad.description, segments.conversion_action_name, metrics.all_conversions, metrics.all_conversions_value
    FROM search_term_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """
    conv_action_columns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Campaign_Type', 'Ad_Group_Labels', 'Campaign_Labels', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Search_Keyword', 'Search_Term', 'Ad_Type', 'Ad_ID',
                        'Headline_1', 'Headline_2', 'Headline_3', 'Description', 'Conversion_Action', 'All_Conversions', 'All_Conversions_Value']

    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    conv_action = pd.DataFrame(columns = conv_action_columns)
    for batch in stream:
        for row in batch.results:
            search_term_view = row.search_term_view
            segments = row.segments
            ad_group = row.ad_group
            campaign = row.campaign
            ad_group_ad = row.ad_group_ad
            customer = row.customer
            metrics = row.metrics
            df_entry = [segments.date, customer.descriptive_name, customer.id, campaign.name, campaign.id, campaign.advertising_channel_type.name, ad_group.labels, campaign.labels, ad_group.type_.name, ad_group.name, ad_group.id, segments.keyword.info.text, search_term_view.search_term, ad_group_ad.ad.type_.name, ad_group_ad.ad.id,
            ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.expanded_text_ad.description, segments.conversion_action_name, metrics.all_conversions, metrics.all_conversions_value]
            
            conv_action = conv_action.append(pd.Series(df_entry, index = conv_action_columns), ignore_index = True)

    conv_action[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Ad_ID']] = conv_action[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Ad_ID']].astype(float)

    for j in conv_action.columns:
        if conv_action[j].dtype == object: 
            conv_action[j] = conv_action[j].astype(str)
        if conv_action[j].dtype == 'int32':
            conv_action[j] = conv_action[j].astype('Int64')


    for j in tmp.columns:
        if tmp[j].dtype == object: 
            tmp[j] = tmp[j].astype(str)
        if tmp[j].dtype == 'int32':
            tmp[j] = tmp[j].astype('Int64')
    for i in set(conv_action['Advertiser']):    
        key = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID',
        'Campaign_Type', 'Ad_Group_Labels', 'Campaign_Labels', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID',
        'Search_Keyword', 'Search_Term', 'Ad_Type', 'Ad_ID', 'Headline_1',
        'Headline_2', 'Headline_3', 'Description']
        pre_Campaign_Conversion = pd.pivot_table(conv_action[conv_action['Advertiser'] == i], index=key, columns='Conversion_Action',
                                                values = [i for i in conv_action.columns if i != 'Conversion_Action' and i not in key],
                                                aggfunc=np.sum, fill_value=0)
        tmp_action = pd.DataFrame(pre_Campaign_Conversion.to_records())
        tmp_action = tmp[tmp['Advertiser'] == i].merge(tmp_action, how = 'left', on = key)
        
        tmp_action_columns = []
        for j in tmp_action.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                tmp_action_columns.append([j])
            else:
                tmp_action_columns.append(j)
        tmp_action_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in tmp_action_columns]
        tmp_action.columns = tmp_action_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Campaign_Conversion
        WHERE Date >= '{start}'
            """
        
        # run the query
        query_job4 = bq_client.query(Q4) 
        job_config = bigquery.LoadJobConfig(
        )
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        job_config.autodetect = True
        bq_client.load_table_from_dataframe(dataframe =  tmp_action[tmp_action['Advertiser'] == i], job_config = job_config, destination = f'{name}.Campaign_Conversion')

        
        Q4 = f"""
        DELETE FROM {name}.Campaign_Summary
        WHERE Date >= '{start}'
            """

        # run the query
        query_job4 = bq_client.query(Q4) 
        job_config = bigquery.LoadJobConfig(
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.autodetect = True
        bq_client.load_table_from_dataframe(dataframe =  tmp[tmp['Advertiser'] == i], job_config = job_config, destination = f'{name}.Campaign_Summary')
        
        
        Q4 = f"""
        DELETE FROM {name}.merge_data
        WHERE Date >= '{start}'
            """

        # run the query
        query_job4 = bq_client.query(Q4) 
        tmp_action = tmp_action.replace(np.nan, 0)
        job_config = bigquery.LoadJobConfig(
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.autodetect = True
        bq_client.load_table_from_dataframe(dataframe =  tmp_action, job_config = job_config, destination = f'{name}.merge_data')

def getShoppingCampaignReport(googleads_client, client_id):
    ga_service = googleads_client.get_service("GoogleAdsService")
    #shopping performance view
    query = f"""
    SELECT segments.date, customer.descriptive_name, customer.id, campaign.name, campaign.id,  campaign.advertising_channel_type, campaign.labels, ad_group.labels, ad_group.type, ad_group.name, ad_group.id, segments.product_type_l1,
    segments.product_type_l2, segments.product_type_l3, segments.product_type_l4, segments.product_type_l5,
    customer.currency_code, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, 
    metrics.conversions_value, metrics.all_conversions, metrics.all_conversions_value 
    FROM shopping_performance_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """

    productcolumns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Campaign_Type', 'Campaign_Labels', 'Ad_Group_Labels', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Product_1', 'Product_2',
                    'Product_3', 'Product_4', 'Product_5', 'Currency_Code', 'Impressions', 'Clicks', 'Cost', 'Conversions', 
                    'Conversions_Value', 'All_Conversions', 'All_Conversions_Value']

    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    product = pd.DataFrame(columns = productcolumns)
    for batch in stream:
        for row in batch.results:
            campaign = row.campaign
            segments = row.segments
            customer = row.customer
            metrics = row.metrics
            ad_group = row.ad_group
            df_entry = [segments.date, customer.descriptive_name, customer.id, campaign.name, campaign.id, campaign.advertising_channel_type.name, campaign.labels, ad_group.labels, ad_group.type_.name, ad_group.name, ad_group.id, segments.product_type_l1,
    segments.product_type_l2, segments.product_type_l3, segments.product_type_l4, segments.product_type_l5,
    customer.currency_code, metrics.impressions, metrics.clicks, metrics.cost_micros/1000000, metrics.conversions, 
    metrics.conversions_value, metrics.all_conversions, metrics.all_conversions_value 
            ]
            product = product.append(pd.Series(df_entry, index = productcolumns), ignore_index = True)


    for j in product.columns:
        if product[j].dtype == 'object': 
            product[j] = product[j].astype(str)
        if product[j].dtype == 'int32':
            product[j] = product[j].astype('Int64')

    for i in set(product['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Shopping_Campaign
        WHERE Date >= '{start}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)  
        
        product[['Impressions','Clicks']] = product[['Impressions','Clicks']].astype(int)
        product[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']] = product[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']].astype(float)   
        job_config = bigquery.LoadJobConfig(
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        schemalist = []
        for j in product.columns:
            if str(product.dtypes[j]) == 'object':
                schemalist.append(bigquery.SchemaField(j, 'STRING'))
            elif str(product.dtypes[j]) == 'int32':
                schemalist.append(bigquery.SchemaField(j, 'INTEGER'))
            else:
                schemalist.append(bigquery.SchemaField(j, str(product.dtypes[j])))
        job_config.schema = schemalist
        bq_client.load_table_from_dataframe(dataframe = product[product['Advertiser'] == i], job_config = job_config, destination = f'{name}.Shopping_Campaign')
      
def getImpressionShareReport(googleads_client, client_id):  
    ga_service = googleads_client.get_service("GoogleAdsService")
    #campaign
    query = f"""
    SELECT
        segments.date,
        customer.descriptive_name, 
        customer.id, 
        campaign.name,
        campaign.id,
        campaign.advertising_channel_type,
        campaign.labels,
        ad_group.type,
        ad_group.name,
        ad_group.id,
        ad_group.labels,
        customer.currency_code,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        metrics.all_conversions,
        metrics.all_conversions_value,
        metrics.content_impression_share,
        metrics.search_rank_lost_impression_share,
        metrics.content_rank_lost_impression_share,
        metrics.search_budget_lost_top_impression_share,
        metrics.search_impression_share,
        metrics.search_top_impression_share,
        metrics.search_absolute_top_impression_share,
        metrics.search_rank_lost_top_impression_share,
        metrics.search_budget_lost_absolute_top_impression_share,
        metrics.search_rank_lost_absolute_top_impression_share
    FROM ad_group 
    WHERE segments.date BETWEEN '{start}' AND '{end}'
        """
    dfcolumns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Campaign_Type', 'Campaign_Labels', 'Ad_Group_Type', 
                'Ad_Group_Name',  'Ad_Group_ID', 'Ad_Group_Labels', 'Currency', 
                'Impressions', 'Clicks', 'Cost', 
                'Conversions', 'Conversion_Value', 'All_Conversions', 'All_Conversion_Value',
                'Display_Impression_Share', 'Search_Rank_Lost_Impression_Share', 
                'Display_Rank_Lost_Impression_Share', 'Search_Budget_Lost_Top_Impression_Share',
                'Search_Impression_Share', 'Search_Top_Impression_Share',
                'Search_Absolute_Top_Impression_Share', 'Search_Rank_Lost_Top_Impression_Share',
                'Search_Budget_Lost_Absolute_Top_Impression_Share',
                'Search_Rank_Lost_Absolute_Top_Impression_Share']


    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    df = pd.DataFrame(columns = dfcolumns)
    for batch in stream:
        for row in batch.results:
            metrics = row.metrics
            ad_group = row.ad_group
            campaign = row.campaign
            customer = row.customer
            segments = row.segments
            df_entry = [
            segments.date,
            customer.descriptive_name, 
            customer.id, 
            campaign.name,
            campaign.id,
            campaign.advertising_channel_type.name,
            campaign.labels,
            ad_group.type_.name,
            ad_group.name,
            ad_group.id,
            ad_group.labels,
            customer.currency_code,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros/1000000,
            metrics.conversions,
            metrics.conversions_value,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.content_impression_share,
            metrics.search_rank_lost_impression_share,
            metrics.content_rank_lost_impression_share,
            metrics.search_budget_lost_top_impression_share,
            metrics.search_impression_share,
            metrics.search_top_impression_share,
            metrics.search_absolute_top_impression_share,
            metrics.search_rank_lost_top_impression_share,
            metrics.search_budget_lost_absolute_top_impression_share,
            metrics.search_rank_lost_absolute_top_impression_share
            ]
            
            df = df.append(pd.Series(df_entry, index = dfcolumns), ignore_index = True)

        
    df[['Impressions','Clicks']] = df[['Impressions','Clicks']].astype(int)
    df[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']] = df[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']].astype(float)
    for j in df.columns:
        if df[j].dtype == 'object': 
            df[j] = df[j].astype(str)
        if df[j].dtype == 'int32':
            df[j] = df[j].astype('Int64')
    for i in set(df['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Impression_Share
        WHERE Date >= '{start}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)     
        
        
        job_config = bigquery.LoadJobConfig(
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        schemalist = []
        for j in df.columns:
            if str(df.dtypes[j]) == 'object':
                schemalist.append(bigquery.SchemaField(j, 'STRING'))
            elif str(df.dtypes[j]) == 'int32':
                schemalist.append(bigquery.SchemaField(j, 'INTEGER'))
            else:
                schemalist.append(bigquery.SchemaField(j, str(df.dtypes[j])))
        job_config.schema = schemalist
        
        
        bq_client.load_table_from_dataframe(dataframe = df[df['Advertiser'] == i], job_config = job_config, destination = f'{name}.Impression_Share')

def getAdGroupAdConversionReport(googleads_client, client_id, Ad_Group_Ad_Summary):
    
    query = f"""
    SELECT
    segments.date,
    customer.descriptive_name, 
    customer.id, 
    campaign.id,
    campaign.name,
    campaign.advertising_channel_sub_type,
    ad_group.name,
    ad_group.type,
    ad_group.id,
    ad_group_ad.labels,
    campaign.labels,
    ad_group_ad.ad.type,
    ad_group_ad.ad.app_ad.descriptions,
    ad_group_ad.ad.app_ad.headlines,
    ad_group_ad.ad.app_engagement_ad.descriptions,
    ad_group_ad.ad.app_engagement_ad.headlines,
    ad_group_ad.ad.app_pre_registration_ad.descriptions,
    ad_group_ad.ad.app_pre_registration_ad.headlines,
    ad_group_ad.ad.call_ad.description1,
    ad_group_ad.ad.call_ad.description2,
    ad_group_ad.ad.call_ad.headline1,
    ad_group_ad.ad.call_ad.headline2,
    ad_group_ad.ad.expanded_dynamic_search_ad.description,
    ad_group_ad.ad.expanded_dynamic_search_ad.description2,
    ad_group_ad.ad.expanded_text_ad.description,
    ad_group_ad.ad.expanded_text_ad.description2,
    ad_group_ad.ad.expanded_text_ad.headline_part1,
    ad_group_ad.ad.expanded_text_ad.headline_part2,
    ad_group_ad.ad.expanded_text_ad.headline_part3,
    ad_group_ad.ad.legacy_responsive_display_ad.description,
    ad_group_ad.ad.legacy_responsive_display_ad.long_headline,
    ad_group_ad.ad.legacy_responsive_display_ad.short_headline,
    ad_group_ad.ad.local_ad.descriptions,
    ad_group_ad.ad.local_ad.headlines,
    ad_group_ad.ad.responsive_display_ad.descriptions,
    ad_group_ad.ad.responsive_display_ad.headlines,
    ad_group_ad.ad.responsive_search_ad.descriptions,
    ad_group_ad.ad.responsive_search_ad.headlines,
    ad_group_ad.ad.shopping_comparison_listing_ad.headline,
    ad_group_ad.ad.smart_campaign_ad.descriptions,
    ad_group_ad.ad.smart_campaign_ad.headlines,
    ad_group_ad.ad.text_ad.description1,
    ad_group_ad.ad.text_ad.description2,
    ad_group_ad.ad.text_ad.headline,
    ad_group_ad.ad.video_ad.in_stream.action_headline,
    ad_group_ad.ad.video_ad.non_skippable.action_headline,
    ad_group_ad.ad.video_ad.out_stream.description,
    ad_group_ad.ad.video_ad.out_stream.headline,
    ad_group_ad.ad.video_responsive_ad.descriptions,
    ad_group_ad.ad.video_responsive_ad.headlines,
    ad_group_ad.ad.video_responsive_ad.long_headlines,
    segments.conversion_action_name,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.conversions,
    metrics.conversions_value
    FROM ad_group_ad
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """


    ad_group_conv_action_columns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_ID', 'Campaign_Name', 'Campaign_Channel_Sub_Type', 'Ad_Group_Name', 'Ad_Group_Type', 'Ad_Group_ID', 'Ad_Group_Ad_Labels', 'Campaign_Labels', 'Ad_Type', 'Descriptions', 
    'Description_1', 'Description_2',  'Headlines', 'Long_Headlines', 'Long_Headline', 'Short_Headline', 'Action_Headline', 'Headline_1', 'Headline_2', 'Headline_3', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions',
    'Active_View_Measurable_Impressions', 'Active_View_Viewability', 'Conversion_Action', 'All_Conversions', 'All_Conversions_Value', 'Conversions', 'Conversions_Value']

    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    ad_group_conv_action = pd.DataFrame(columns = ad_group_conv_action_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            ad_group_ad = row.ad_group_ad
            ad_group_ad_type = ad_group_ad.ad.type_.name
            ad_group_ad = util.convert_proto_plus_to_protobuf(ad_group_ad)
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            df_entry = [
            segments.date,
            customer.descriptive_name, 
            customer.id, 
            campaign.id,
            campaign.name,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.name,
            ad_group.type_.name,
            ad_group.id,
            ad_group_ad.labels,
            campaign.labels,
            ad_group_ad_type,
            str(ad_group_ad.ad.app_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.app_engagement_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.app_pre_registration_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_display_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_search_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.video_responsive_ad.descriptions).replace('[]', '') +
            str(ad_group_ad.ad.smart_campaign_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.local_ad.descriptions).replace('[]', ''),
            str(ad_group_ad.ad.call_ad.description1).replace('[]', '') + 
            str(ad_group_ad.ad.text_ad.description1).replace('[]', '') + 
            str(ad_group_ad.ad.expanded_dynamic_search_ad.description).replace('[]', '') + 
            str(ad_group_ad.ad.expanded_text_ad.description).replace('[]', '') + 
            str(ad_group_ad.ad.legacy_responsive_display_ad.description).replace('[]', '') + 
            str(ad_group_ad.ad.video_ad.out_stream.description).replace('[]', ''),
            str(ad_group_ad.ad.expanded_text_ad.description2).replace('[]', '')+ 
            str(ad_group_ad.ad.call_ad.description2).replace('[]', '') + 
            str(ad_group_ad.ad.expanded_dynamic_search_ad.description2).replace('[]', '') + 
            str(ad_group_ad.ad.text_ad.description2).replace('[]', ''),
            str(ad_group_ad.ad.app_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.app_engagement_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.app_pre_registration_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.local_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_display_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_search_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.smart_campaign_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.video_responsive_ad.headlines).replace('[]', ''), 
            str(ad_group_ad.ad.video_responsive_ad.long_headlines).replace('[]', ''),
            str(ad_group_ad.ad.legacy_responsive_display_ad.long_headline).replace('[]', ''),
            str(ad_group_ad.ad.legacy_responsive_display_ad.short_headline).replace('[]', ''),
            str(ad_group_ad.ad.video_ad.in_stream.action_headline).replace('[]', '') + 
            str(ad_group_ad.ad.video_ad.non_skippable.action_headline).replace('[]', ''),
            str(ad_group_ad.ad.expanded_text_ad.headline_part1).replace('[]', '') + 
            str(ad_group_ad.ad.call_ad.headline1).replace('[]', '') + 
            str(ad_group_ad.ad.shopping_comparison_listing_ad.headline).replace('[]', '') + 
            str(ad_group_ad.ad.text_ad.headline).replace('[]', '') + 
            str(ad_group_ad.ad.video_ad.out_stream.headline).replace('[]', ''), 
            str(ad_group_ad.ad.expanded_text_ad.headline_part2).replace('[]', '') + 
            str(ad_group_ad.ad.call_ad.headline2).replace('[]', ''), 
            str(ad_group_ad.ad.expanded_text_ad.headline_part3).replace('[]', ''),
            metrics.absolute_top_impression_percentage,
            metrics.active_view_impressions,
            metrics.active_view_measurable_impressions,
            metrics.active_view_viewability,
            segments.conversion_action_name,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.conversions,
            metrics.conversions_value,
            ]
            
            ad_group_conv_action = ad_group_conv_action.append(pd.Series(df_entry, index = ad_group_conv_action_columns), ignore_index = True)
            
    for i in ad_group_conv_action.columns:
            if 'object' in str(type(ad_group_conv_action[i].dtype)):
                ad_group_conv_action[i] = ad_group_conv_action[i].apply(lambda x: str(x))
    decode_columns = ['Descriptions', 'Description_1', 'Description_2',  'Headlines', 'Long_Headlines', 'Long_Headline', 'Short_Headline', 'Action_Headline', 'Headline_1', 'Headline_2', 'Headline_3']
    for i in decode_columns:
        ad_group_conv_action[i] = ad_group_conv_action[i].apply(lambda x: x.encode().decode('unicode_escape').encode("raw_unicode_escape").decode('utf-8'))
    ad_group_conv_action['Conversion_Action'] = ad_group_conv_action['Conversion_Action'].str.lower().apply(lambda x: re.sub('[^A-Za-z0-9_\u4e00-\u9fff]+', '_', x))
    for i in set(ad_group_conv_action['Advertiser']):    
        key = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_ID', 'Campaign_Name', 'Campaign_Channel_Sub_Type', 'Ad_Group_Name', 'Ad_Group_Type', 'Ad_Group_ID', 'Ad_Group_Ad_Labels', 'Campaign_Labels', 'Ad_Type', 'Descriptions', 
    'Description_1', 'Description_2',  'Headlines', 'Long_Headlines', 'Long_Headline', 'Short_Headline', 'Action_Headline', 'Headline_1', 'Headline_2', 'Headline_3', 'Conversion_Action']
        ad_group_conv_action = ad_group_conv_action.groupby(key).sum()
        ad_group_conv_action = pd.DataFrame(ad_group_conv_action.to_records())
        key = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_ID', 'Campaign_Name', 'Campaign_Channel_Sub_Type',  'Ad_Group_Name', 'Ad_Group_Type', 'Ad_Group_ID', 'Ad_Group_Ad_Labels', 'Campaign_Labels', 'Ad_Type', 'Descriptions', 
    'Description_1', 'Description_2',  'Headlines', 'Long_Headlines', 'Long_Headline', 'Short_Headline', 'Action_Headline', 'Headline_1', 'Headline_2', 'Headline_3']
        #Ad Group Ad with keyword
        pre_Campaign_Conversion = pd.pivot_table(ad_group_conv_action[ad_group_conv_action['Advertiser'] == i], index=key, columns='Conversion_Action',
                                                values = [k for k in ad_group_conv_action.columns if k != 'Conversion_Action' and k not in key],
                                                aggfunc=np.sum, fill_value=0)

        pre_Campaign_Conversion = pre_Campaign_Conversion.groupby(key).sum()

        pre_Campaign_Conversion = pd.DataFrame(pre_Campaign_Conversion.to_records())
        
        
        keyword_ad_group_column = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                keyword_ad_group_column.append([j])
            else:
                keyword_ad_group_column.append(j)
        keyword_ad_group_column = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in keyword_ad_group_column]
        pre_Campaign_Conversion.columns = keyword_ad_group_column
        
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        pre_Campaign_Conversion[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID']] = pre_Campaign_Conversion[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID']].astype(float)

        try: 
            sql = f"""
                SELECT * FROM {name}.Ad_Group_Ad_Conversion
                WHERE Date < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api= True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
                    
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Ad_Conversion')
        except: 
            DATA = pre_Campaign_Conversion
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Ad_Conversion')


        
        pre_Campaign_Conversion = Ad_Group_Ad_Summary[Ad_Group_Ad_Summary['Advertiser'] == i].merge(pre_Campaign_Conversion, how = 'left', on = key)

        ad_group_conv_action_columns = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                ad_group_conv_action_columns.append([j])
            else:
                ad_group_conv_action_columns.append(j)
        ad_group_conv_action_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in ad_group_conv_action_columns]
        pre_Campaign_Conversion.columns = ad_group_conv_action_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)

        pre_Campaign_Conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        
        try: 
            sql = f"""
                SELECT * FROM {name}.Ad_Group_Ad_Merged
                WHERE Date < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api= True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Ad_Merged')
        except: 
            DATA = pre_Campaign_Conversion
            
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Ad_Merged')
    
def getAdGroupAdSummaryReport(googleads_client, client_id):
    query = f"""
    SELECT
    segments.date,
    customer.descriptive_name, 
    customer.id, 
    campaign.id,
    campaign.name,
    campaign.advertising_channel_sub_type,
    ad_group.name,
    ad_group.type,
    ad_group.id,
    ad_group_ad.labels,
    campaign.labels,
    ad_group_ad.ad.type,
    ad_group_ad.ad.app_ad.descriptions,
    ad_group_ad.ad.app_ad.headlines,
    ad_group_ad.ad.app_engagement_ad.descriptions,
    ad_group_ad.ad.app_engagement_ad.headlines,
    ad_group_ad.ad.app_pre_registration_ad.descriptions,
    ad_group_ad.ad.app_pre_registration_ad.headlines,
    ad_group_ad.ad.call_ad.description1,
    ad_group_ad.ad.call_ad.description2,
    ad_group_ad.ad.call_ad.headline1,
    ad_group_ad.ad.call_ad.headline2,
    ad_group_ad.ad.expanded_dynamic_search_ad.description,
    ad_group_ad.ad.expanded_dynamic_search_ad.description2,
    ad_group_ad.ad.expanded_text_ad.description,
    ad_group_ad.ad.expanded_text_ad.description2,
    ad_group_ad.ad.expanded_text_ad.headline_part1,
    ad_group_ad.ad.expanded_text_ad.headline_part2,
    ad_group_ad.ad.expanded_text_ad.headline_part3,
    ad_group_ad.ad.legacy_responsive_display_ad.description,
    ad_group_ad.ad.legacy_responsive_display_ad.long_headline,
    ad_group_ad.ad.legacy_responsive_display_ad.short_headline,
    ad_group_ad.ad.local_ad.descriptions,
    ad_group_ad.ad.local_ad.headlines,
    ad_group_ad.ad.responsive_display_ad.descriptions,
    ad_group_ad.ad.responsive_display_ad.headlines,
    ad_group_ad.ad.responsive_search_ad.descriptions,
    ad_group_ad.ad.responsive_search_ad.headlines,
    ad_group_ad.ad.shopping_comparison_listing_ad.headline,
    ad_group_ad.ad.smart_campaign_ad.descriptions,
    ad_group_ad.ad.smart_campaign_ad.headlines,
    ad_group_ad.ad.text_ad.description1,
    ad_group_ad.ad.text_ad.description2,
    ad_group_ad.ad.text_ad.headline,
    ad_group_ad.ad.video_ad.in_stream.action_headline,
    ad_group_ad.ad.video_ad.non_skippable.action_headline,
    ad_group_ad.ad.video_ad.out_stream.description,
    ad_group_ad.ad.video_ad.out_stream.headline,
    ad_group_ad.ad.video_responsive_ad.descriptions,
    ad_group_ad.ad.video_responsive_ad.headlines,
    ad_group_ad.ad.video_responsive_ad.long_headlines,
    metrics.absolute_top_impression_percentage,
    metrics.active_view_impressions,
    metrics.active_view_measurable_impressions,
    metrics.active_view_viewability,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.clicks,
    metrics.conversions,
    metrics.conversions_value,
    metrics.engagements,
    metrics.cost_micros,
    metrics.impressions,
    metrics.interactions,
    metrics.top_impression_percentage,
    metrics.video_quartile_p100_rate,
    metrics.video_quartile_p25_rate,
    metrics.video_quartile_p50_rate,
    metrics.video_quartile_p75_rate,
    metrics.video_view_rate,
    metrics.video_views
    FROM ad_group_ad
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """

    ad_group_ad_summary_columns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_ID', 'Campaign_Name',  'Campaign_Channel_Sub_Type', 'Ad_Group_Name', 'Ad_Group_Type', 'Ad_Group_ID', 'Ad_Group_Ad_Labels', 'Campaign_Labels', 'Ad_Type', 'Descriptions', 
    'Description_1', 'Description_2',  'Headlines', 'Long_Headlines', 'Long_Headline', 'Short_Headline', 'Action_Headline', 'Headline_1', 'Headline_2', 'Headline_3', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions',
    'Active_View_Measurable_Impressions', 'Active_View_Viewability', 'All_Conversions', 'All_Conversions_Value', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Conversions_Value', 'Engagements',
    'Interactions', 'Top_Impression_Percentage', 'Video_Quartile_p25_rate', 'Video_Quartile_p50_rate', 'Video_Quartile_p75_rate', 'Video_Quartile_p100_rate', 'Video_View_Rate', 'Video_Views']


    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    ad_group_ad_summary = pd.DataFrame(columns = ad_group_ad_summary_columns)

    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            ad_group_ad = row.ad_group_ad
            ad_group_ad_type = ad_group_ad.ad.type_.name
            ad_group_ad = util.convert_proto_plus_to_protobuf(ad_group_ad)
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            df_entry = [
            segments.date,
            customer.descriptive_name, 
            customer.id, 
            campaign.id,
            campaign.name,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.name,
            ad_group.type_.name,
            ad_group.id, 
            ad_group_ad.labels,
            campaign.labels,
            ad_group_ad_type,
            str(ad_group_ad.ad.app_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.app_engagement_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.app_pre_registration_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_display_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_search_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.video_responsive_ad.descriptions).replace('[]', '') +
            str(ad_group_ad.ad.smart_campaign_ad.descriptions).replace('[]', '') + 
            str(ad_group_ad.ad.local_ad.descriptions).replace('[]', ''),
            str(ad_group_ad.ad.call_ad.description1).replace('[]', '') + 
            str(ad_group_ad.ad.text_ad.description1).replace('[]', '') + 
            str(ad_group_ad.ad.expanded_dynamic_search_ad.description).replace('[]', '') + 
            str(ad_group_ad.ad.expanded_text_ad.description).replace('[]', '') + 
            str(ad_group_ad.ad.legacy_responsive_display_ad.description).replace('[]', '') + 
            str(ad_group_ad.ad.video_ad.out_stream.description).replace('[]', ''),
            str(ad_group_ad.ad.expanded_text_ad.description2).replace('[]', '')+ 
            str(ad_group_ad.ad.call_ad.description2).replace('[]', '') + 
            str(ad_group_ad.ad.expanded_dynamic_search_ad.description2).replace('[]', '') + 
            str(ad_group_ad.ad.text_ad.description2).replace('[]', ''),
            str(ad_group_ad.ad.app_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.app_engagement_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.app_pre_registration_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.local_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_display_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.responsive_search_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.smart_campaign_ad.headlines).replace('[]', '') + 
            str(ad_group_ad.ad.video_responsive_ad.headlines).replace('[]', ''), 
            str(ad_group_ad.ad.video_responsive_ad.long_headlines).replace('[]', ''),
            str(ad_group_ad.ad.legacy_responsive_display_ad.long_headline).replace('[]', ''),
            str(ad_group_ad.ad.legacy_responsive_display_ad.short_headline).replace('[]', ''),
            str(ad_group_ad.ad.video_ad.in_stream.action_headline).replace('[]', '') + 
            str(ad_group_ad.ad.video_ad.non_skippable.action_headline).replace('[]', ''),
            str(ad_group_ad.ad.expanded_text_ad.headline_part1).replace('[]', '') + 
            str(ad_group_ad.ad.call_ad.headline1).replace('[]', '') + 
            str(ad_group_ad.ad.shopping_comparison_listing_ad.headline).replace('[]', '') + 
            str(ad_group_ad.ad.text_ad.headline).replace('[]', '') + 
            str(ad_group_ad.ad.video_ad.out_stream.headline).replace('[]', ''), 
            str(ad_group_ad.ad.expanded_text_ad.headline_part2).replace('[]', '') + 
            str(ad_group_ad.ad.call_ad.headline2).replace('[]', ''), 
            str(ad_group_ad.ad.expanded_text_ad.headline_part3).replace('[]', ''),
            metrics.absolute_top_impression_percentage,
            metrics.active_view_impressions,
            metrics.active_view_measurable_impressions,
            metrics.active_view_viewability,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.cost_micros/1000000,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.engagements,
            metrics.interactions,
            metrics.top_impression_percentage,
            metrics.video_quartile_p25_rate,
            metrics.video_quartile_p50_rate,
            metrics.video_quartile_p75_rate,
            metrics.video_quartile_p100_rate,
            metrics.video_view_rate,
            metrics.video_views
            ]
            
            ad_group_ad_summary = ad_group_ad_summary.append(pd.Series(df_entry, index = ad_group_ad_summary_columns), ignore_index = True)

    decode_columns = [ 'Descriptions', 'Description_1', 'Description_2',  'Headlines', 'Long_Headlines', 'Long_Headline', 'Short_Headline', 'Action_Headline', 'Headline_1', 'Headline_2', 'Headline_3']
    for i in decode_columns:
        ad_group_ad_summary[i] = ad_group_ad_summary[i].apply(lambda x: x.encode().decode('unicode_escape').encode("raw_unicode_escape").decode('utf-8'))
    for column in ad_group_ad_summary.columns:
        if(type(ad_group_ad_summary[column]) == 'bytes'):
          ad_group_ad_summary[column] = ad_group_ad_summary[column].apply(lambda x: x.decode('UTF-8'))
    for i in ad_group_ad_summary.columns:
            if 'object' in str(type(ad_group_ad_summary[i].dtype)):
                ad_group_ad_summary[i] = ad_group_ad_summary[i].apply(lambda x: str(x))
    
    ad_group_ad_summary[['Impressions','Clicks']] = ad_group_ad_summary[['Impressions','Clicks']].astype(int)
    ad_group_ad_summary[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions',
    'Active_View_Measurable_Impressions', 'Active_View_Viewability', 'All_Conversions', 'All_Conversions_Value', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Conversions_Value', 'Engagements',
    'Interactions', 'Top_Impression_Percentage', 'Video_Quartile_p25_rate', 'Video_Quartile_p50_rate', 'Video_Quartile_p75_rate', 'Video_Quartile_p100_rate', 'Video_View_Rate', 'Video_Views']] = ad_group_ad_summary[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions', 'Active_View_Measurable_Impressions', 'Active_View_Viewability', 'All_Conversions', 'All_Conversions_Value', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Conversions_Value', 'Engagements',
    'Interactions', 'Top_Impression_Percentage', 'Video_Quartile_p25_rate', 'Video_Quartile_p50_rate', 'Video_Quartile_p75_rate', 'Video_Quartile_p100_rate', 'Video_View_Rate', 'Video_Views']].astype(float)
        
    for i in set(ad_group_ad_summary['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Ad_Group_Ad_Summary
        WHERE Date BETWEEN '{start}' and '{end}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)     

        ad_group_ad_summary[['Impressions','Clicks']] = ad_group_ad_summary[['Impressions','Clicks']].astype(int)
        ad_group_ad_summary[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions',
        'Active_View_Measurable_Impressions', 'Active_View_Viewability', 'All_Conversions', 'All_Conversions_Value', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Conversions_Value', 'Engagements',
        'Interactions', 'Top_Impression_Percentage', 'Video_Quartile_p25_rate', 'Video_Quartile_p50_rate', 'Video_Quartile_p75_rate', 'Video_Quartile_p100_rate', 'Video_View_Rate', 'Video_Views']] = ad_group_ad_summary[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions', 'Active_View_Measurable_Impressions', 'Active_View_Viewability', 'All_Conversions', 'All_Conversions_Value', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Conversions_Value', 'Engagements',
        'Interactions', 'Top_Impression_Percentage', 'Video_Quartile_p25_rate', 'Video_Quartile_p50_rate', 'Video_Quartile_p75_rate', 'Video_Quartile_p100_rate', 'Video_View_Rate', 'Video_Views']].astype(float)
        
        job_config = bigquery.LoadJobConfig(
            
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        
        bq_client.load_table_from_dataframe(dataframe = ad_group_ad_summary[ad_group_ad_summary['Advertiser'] == i], job_config = job_config, destination = f'{name}.Ad_Group_Ad_Summary')
       
    
    return ad_group_ad_summary

def getKeywordSummaryReport(googleads_client, client_id):
    query = f"""
    SELECT segments.week, segments.month, customer.descriptive_name, customer.id, campaign.name, campaign.id, campaign.advertising_channel_sub_type,
    ad_group_criterion.labels, ad_group.name, ad_group.id, ad_group_criterion.keyword.text, metrics.impressions, 
    metrics.clicks, metrics.cost_micros, 
    metrics.conversions,
    metrics.conversions_value,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.search_rank_lost_impression_share,
    metrics.search_budget_lost_top_impression_share,
    metrics.search_impression_share,
    metrics.search_top_impression_share,
    metrics.search_absolute_top_impression_share,
    metrics.search_rank_lost_top_impression_share,
    metrics.search_budget_lost_absolute_top_impression_share,
    metrics.search_rank_lost_absolute_top_impression_share
    FROM keyword_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """
    conv_action_columns = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name',  'Campaign_ID', 'Campaign_Channel_Sub_Type', 'Ad_Group_Criterion_Labels', 'Ad_Group_Name', 'Ad_Group_ID', 'Keyword', 'Impressions', 'Clicks',
    'Cost', 'Conversions', 'Conversions_Value', 'All_Conversions', 'All_Conversion_Value', 'Search_Rank_Lost_Impression_Share', 'Search_Budget_Lost_Top_Impression_Share', 'Search_Impression_Share', 'Search_Top_Impression_Share'
    , 'Search_Absolute_Top_Impression_Share', 'Search_Rank_Lost_Top_Impression_Share', 'Search_Budget_Lost_Absolute_Top_Impression_Share', 'Search_Rank_Lost_Absolute_Top_Impression_Share']

    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    tmp = pd.DataFrame(columns = conv_action_columns)

    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            ad_group_criterion = row.ad_group_criterion
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            ad_group_criterion = row.ad_group_criterion
            df_entry = [segments.week, segments.month,  customer.descriptive_name, customer.id, campaign.name, campaign.id, vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group_criterion.labels,  ad_group.name, ad_group.id, ad_group_criterion.keyword.text, metrics.impressions, 
            metrics.clicks, metrics.cost_micros/1000000, metrics.conversions, metrics.conversions_value,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.search_rank_lost_impression_share,
            metrics.search_budget_lost_top_impression_share,
            metrics.search_impression_share,
            metrics.search_top_impression_share,
            metrics.search_absolute_top_impression_share,
            metrics.search_rank_lost_top_impression_share,
            metrics.search_budget_lost_absolute_top_impression_share,
            metrics.search_rank_lost_absolute_top_impression_share]
            
            tmp = tmp.append(pd.Series(df_entry, index = conv_action_columns), ignore_index = True)
    for i in tmp.columns:
            if 'object' in str(type(tmp[i].dtype)):
                tmp[i] = tmp[i].apply(lambda x: str(x))
                
    for i in set(tmp['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Keyword_Summary
        WHERE Week BETWEEN '{start}' AND '{end}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)     
        
        tmp[['Impressions','Clicks']] = tmp[['Impressions','Clicks']].astype(int)
        tmp[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']] = tmp[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']].astype(float)
        job_config = bigquery.LoadJobConfig(
            
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        
        bq_client.load_table_from_dataframe(dataframe = tmp[tmp['Advertiser'] == i], job_config = job_config, destination = f'{name}.Keyword_Summary')
    return tmp

def getKeywordConversionReport(googleads_client, client_id, keyword_summary):
    query = f"""
    SELECT
    segments.week,
    segments.month,
    customer.descriptive_name, 
    customer.id, 
    campaign.id,
    campaign.name,
    campaign.advertising_channel_sub_type,
    ad_group.name,
    ad_group.type,
    ad_group.id,
    ad_group_ad.ad.id,
    ad_group_ad.ad.type,
    segments.keyword.info.text,
    segments.conversion_action_name,
    metrics.conversions,
    metrics.conversions_value,
    metrics.all_conversions,
    metrics.all_conversions_value
    FROM ad_group_ad
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """


    keyword_ad_group_column = ['Week','Month', 'Advertiser',  'Customer_ID', 'Campaign_ID', 'Campaign_Name' , 'Campaign_Channel_Sub_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Ad_ID', 'Ad_Type', 'Keyword',
                        'Conversion_Action', 'Conversions', 'Conversion_Value', 'All_Conversions', 'All_Conversion_Value']

    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    keyword_ad_group = pd.DataFrame(columns = keyword_ad_group_column)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            ad_group_ad = row.ad_group_ad
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            df_entry = [
            segments.week,
            segments.month,
            customer.descriptive_name, 
            customer.id, 
            campaign.id,
            campaign.name,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.name,
            ad_group.id,
            ad_group_ad.ad.id,
            ad_group_ad.ad.type_.name,
            segments.keyword.info.text,
            segments.conversion_action_name,
            metrics.conversions,
            metrics.conversions_value,
            metrics.all_conversions,
            metrics.all_conversions_value]
            
            keyword_ad_group = keyword_ad_group.append(pd.Series(df_entry, index = keyword_ad_group_column), ignore_index = True)

    for i in keyword_ad_group.columns:
            if 'object' in str(type(keyword_ad_group[i].dtype)):
                keyword_ad_group[i] = keyword_ad_group[i].apply(lambda x: str(x))
                

    keyword_ad_group['Conversion_Action'] = keyword_ad_group['Conversion_Action'].str.lower().apply(lambda x: re.sub('[^A-Za-z0-9_\u4e00-\u9fff]+', '_', x))
    
    
    for i in set(keyword_ad_group['Advertiser']):    
        key = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID',   'Campaign_Channel_Sub_Type',  'Ad_Group_Name', 'Ad_Group_ID', 'Keyword', 'Conversion_Action']
        keyword_ad_group = keyword_ad_group.groupby(key).sum()
        keyword_ad_group = pd.DataFrame(keyword_ad_group.to_records())
        #Ad Group Ad with keyword
        key = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID',  'Campaign_Channel_Sub_Type',  'Ad_Group_Name', 'Ad_Group_ID', 'Keyword']
        pre_Campaign_Conversion = pd.pivot_table(keyword_ad_group[keyword_ad_group['Advertiser'] == i], index=key, columns='Conversion_Action',
                                                values = [i for i in keyword_ad_group.columns if i != 'Conversion_Action' and i not in key],
                                                aggfunc=np.sum, fill_value=0)
        pre_Campaign_Conversion = pre_Campaign_Conversion.groupby(key).sum()
        pre_Campaign_Conversion = pd.DataFrame(pre_Campaign_Conversion.to_records())

        keyword_ad_group_column = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                keyword_ad_group_column.append([j])
            else:
                keyword_ad_group_column.append(j)
        keyword_ad_group_column = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in keyword_ad_group_column]
        pre_Campaign_Conversion.columns = keyword_ad_group_column
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        keyword_conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        
        try: 
            sql = f"""
                SELECT * FROM {name}.Keyword_Conversion
                WHERE Week < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api= True)
            DATA = keyword_conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Keyword_Conversion')
        except: 
            DATA = keyword_conversion
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Keyword_Conversion')

        key = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Campaign_Channel_Sub_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Keyword']

        keyword_conversion[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID']] = keyword_conversion[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID']].astype(float)
        keyword_merge= keyword_summary[keyword_summary['Advertiser'] == i].merge(keyword_conversion, how = 'left', on = key)

        keyword_ad_group_column = []
        for j in keyword_merge.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                keyword_ad_group_column.append([j])
            else:
                keyword_ad_group_column.append(j)
        keyword_ad_group_column = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in keyword_ad_group_column]
        keyword_merge.columns = keyword_ad_group_column
        name = re.sub(r'[^a-zA-Z_0-9]','',i)

        keyword_merge[['Impressions','Clicks']] = keyword_merge[['Impressions','Clicks']].astype(int)
        keyword_merge[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']] = keyword_merge[[ 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID', 'Cost']].astype(float)

        try: 
            sql = f"""
                SELECT * FROM {name}.Keyword_Merged
                WHERE Week < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = keyword_merge
            DATA = DATA.append(hist_df, ignore_index = True)
            
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Keyword_Merged')
        except: 
            DATA = keyword_merge
            
            job_config = bigquery.LoadJobConfig(
                
            )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Keyword_Merged')

def getSearchTermSummaryReport(googleads_client, client_id):
    query = f"""
    SELECT
    segments.week,
    segments.month,
    customer.descriptive_name,
    customer.id,
    campaign.name,
    campaign.id,
    ad_group_ad.labels, 
    ad_group.labels, 
    campaign.labels,
    campaign.advertising_channel_sub_type,
    ad_group.type,
    ad_group.name,
    ad_group.id,
    search_term_view.search_term,
    segments.keyword.info.text,
    metrics.absolute_top_impression_percentage,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.clicks,
    metrics.conversions,
    metrics.conversions_value,
    metrics.cost_micros,
    metrics.engagements,
    metrics.impressions,
    metrics.interaction_event_types,
    metrics.interactions,
    metrics.top_impression_percentage,
    metrics.video_quartile_p25_rate,
    metrics.video_quartile_p50_rate,
    metrics.video_quartile_p75_rate,
    metrics.video_quartile_p100_rate,
    metrics.video_views,
    metrics.view_through_conversions
    FROM search_term_view
    where segments.date BETWEEN '{start}' AND '{end}'
    """
    search_term_summary_columns = ['Week', 'Month', 'Advertiser', 'Customer_ID','Campaign_Name', 'Campaign_ID', 'Ad_Group_Ad_Labels', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Search_Term', 
    'Keyword', 'Absolute_Top_Impression_Percentage', 'All_Conversions', 'All_Conversions_Value', 'Clicks', 'Conversions', 'Conversions_Value', 'Cost', 'Engagements', 'Impressions', 'Interaction_Event_Types', 
    'Interactions', 'Top_Impression_Percentage', 'video_quartile_p25_rate', 'video_quartile_p50_rate', 'video_quartile_p75_rate', 'video_quartile_p100_rate',
    'Video_Views', 'View_Through_Conversions']


    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    search_term_summary = pd.DataFrame(columns = search_term_summary_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            search_term_view = row.search_term_view
            ad_group_ad = row.ad_group_ad
            df_entry = [
            segments.week,
            segments.month,
            customer.descriptive_name,
            customer.id,
            campaign.name,
            campaign.id,
            ad_group_ad.labels, 
            ad_group.labels, 
            campaign.labels,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.type_.name,
            ad_group.name,
            ad_group.id,
            search_term_view.search_term,
            segments.keyword.info.text,
            metrics.absolute_top_impression_percentage,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_micros/1000000,
            metrics.engagements,
            metrics.impressions,
            vars(metrics.interaction_event_types[0])['_name_'] if len(metrics.interaction_event_types) > 0 else '',
            metrics.interactions,
            metrics.top_impression_percentage,
            metrics.video_quartile_p25_rate,
            metrics.video_quartile_p50_rate,
            metrics.video_quartile_p75_rate,
            metrics.video_quartile_p100_rate,
            metrics.video_views,
            metrics.view_through_conversions
            ]
            
            search_term_summary = search_term_summary.append(pd.Series(df_entry, index = search_term_summary_columns), ignore_index = True)
    for i in search_term_summary.columns:
            if 'object' in str(type(search_term_summary[i].dtype)):
                search_term_summary[i] = search_term_summary[i].apply(lambda x: str(x))
                
    for i in set(search_term_summary['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Search_Term_Summary
        WHERE Week BETWEEN '{start}' and '{end}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)     
        
        search_term_summary[['Impressions','Clicks', 'Interactions', 'Engagements','Video_Views', 'View_Through_Conversions']] = search_term_summary[['Impressions','Clicks','Interactions', 'Engagements','Video_Views', 'View_Through_Conversions']].astype(int)
        search_term_summary[['Customer_ID', 'Campaign_ID','Ad_Group_ID', 'Cost']] = search_term_summary[[ 'Customer_ID', 'Campaign_ID','Ad_Group_ID', 'Cost']].astype(float)
        job_config = bigquery.LoadJobConfig(
            
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

        bq_client.load_table_from_dataframe(dataframe = search_term_summary[search_term_summary['Advertiser'] == i], job_config = job_config, destination = f'{name}.Search_Term_Summary')
    return search_term_summary

def getSearchTermConversionReport(googleads_client, client_id, search_term_summary):
    query = f"""
    SELECT
    segments.week,
    segments.month,
    customer.descriptive_name,
    customer.id,
    campaign.name,
    campaign.id,
    ad_group_ad.labels, 
    ad_group.labels, 
    campaign.labels,
    campaign.advertising_channel_sub_type,
    ad_group.type,
    ad_group.name,
    ad_group.id,
    search_term_view.search_term,
    segments.keyword.info.text,
    segments.conversion_action_name, 
    metrics.all_conversions, 
    metrics.all_conversions_value, 
    metrics.conversions, 
    metrics.conversions_value 
    FROM search_term_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """

    search_term_conversion_columns = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Ad_Group_Ad_Labels', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type',
    'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Search_Term', 'Keyword',
    'Conversion_Action', 'All_Conversions', 'All_Conversions_Value', 'Conversions', 'Conversions_Value']


    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    search_term_conversion = pd.DataFrame(columns = search_term_conversion_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group_ad = row.ad_group_ad
            ad_group = row.ad_group
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            search_term_view = row.search_term_view
            df_entry = [
            segments.week,
            segments.month,
            customer.descriptive_name,
            customer.id,
            campaign.name,
            campaign.id,
            ad_group_ad.labels, 
            ad_group.labels, 
            campaign.labels,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.type_.name,
            ad_group.name,
            ad_group.id,
            search_term_view.search_term,
            segments.keyword.info.text,
            segments.conversion_action_name, 
            metrics.all_conversions, 
            metrics.all_conversions_value, 
            metrics.conversions, 
            metrics.conversions_value 
            ]
            
            search_term_conversion = search_term_conversion.append(pd.Series(df_entry, index = search_term_conversion_columns), ignore_index = True)
    for i in search_term_conversion.columns:
            if 'object' in str(type(search_term_conversion[i].dtype)):
                search_term_conversion[i] = search_term_conversion[i].apply(lambda x: str(x))
                
    search_term_conversion['Conversion_Action'] = search_term_conversion['Conversion_Action'].str.lower().apply(lambda x: re.sub('[^A-Za-z0-9_\u4e00-\u9fff]+', '_', x))
    
    for i in set(search_term_conversion['Advertiser']):    

        key = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Ad_Group_Ad_Labels', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type',
        'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Keyword', 'Conversion_Action']
        search_term_conversion = search_term_conversion.groupby(key).sum()
        search_term_conversion = pd.DataFrame(search_term_conversion.to_records())
        #Ad Group Ad with keyword
        key = ['Week', 'Month', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Ad_Group_Ad_Labels', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Keyword']
        pre_Campaign_Conversion = pd.pivot_table(search_term_conversion[search_term_conversion['Advertiser'] == i], index=key, columns='Conversion_Action',
                                                values = [i for i in search_term_conversion.columns if i != 'Conversion_Action' and i not in key],
                                                aggfunc=np.sum, fill_value=0)
        pre_Campaign_Conversion = pre_Campaign_Conversion.groupby(key).sum()
        pre_Campaign_Conversion = pd.DataFrame(pre_Campaign_Conversion.to_records())
        pre_Campaign_Conversion[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID' ]] = pre_Campaign_Conversion[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID' ]].astype('float')
        search_term_conversion_columns = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                search_term_conversion_columns.append([j])
            else:
                search_term_conversion_columns.append(j)
        search_term_conversion_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in search_term_conversion_columns]
        pre_Campaign_Conversion.columns = search_term_conversion_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        pre_Campaign_Conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        try: 
            sql = f"""
                SELECT * FROM {name}.Search_Term_Conversion
                WHERE Week < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Search_Term_Conversion')
            
        except: 
            DATA = pre_Campaign_Conversion
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Search_Term_Conversion')


        pre_Campaign_Conversion = search_term_summary[search_term_summary['Advertiser'] == i].merge(pre_Campaign_Conversion, how = 'left', on = key)

        ad_group_conv_action_columns = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                ad_group_conv_action_columns.append([j])
            else:
                ad_group_conv_action_columns.append(j)
        ad_group_conv_action_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in ad_group_conv_action_columns]
        pre_Campaign_Conversion.columns = ad_group_conv_action_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)

        pre_Campaign_Conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        
        try: 
            sql = f"""
                SELECT * FROM {name}.Search_Term_Merged
                WHERE Week < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Search_Term_Merged')
        except: 
            DATA = pre_Campaign_Conversion
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Search_Term_Merged')

def getShoppingConversionReport(googleads_client, client_id, shopping_summary):
        
    query = f"""
    SELECT
    segments.date,
    customer.descriptive_name,
    customer.id,
    campaign.name,
    campaign.id,
    ad_group.labels,
    campaign.labels,
    campaign.advertising_channel_sub_type,
    ad_group.type,
    ad_group.name,
    ad_group.id,
    segments.product_title,
    segments.product_type_l1,
    segments.product_type_l2,
    segments.product_type_l3,
    segments.product_type_l4,
    segments.product_type_l5,
    segments.conversion_action_name,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.conversions,
    metrics.conversions_value
    FROM shopping_performance_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """

    shopping_conversions_columns = ['Date', 'Advertiser', 'Customer_ID','Campaign_Name', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Product_Title', 'Product_Type_1', 
    'Product_Type_2', 'Product_Type_3', 'Product_Type_4', 'Product_Type_5', 'Conversion_Action', 'All_Conversions', 'All_Conversions_Value', 'Conversions', 'Conversions_Value']


    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    shopping_conversions = pd.DataFrame(columns = shopping_conversions_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            df_entry = [
            segments.date,
            customer.descriptive_name,
            customer.id,
            campaign.name,
            campaign.id,
            ad_group.labels,
            campaign.labels,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.type_.name,
            ad_group.name,
            ad_group.id,
            segments.product_title,
            segments.product_type_l1,
            segments.product_type_l2,
            segments.product_type_l3,
            segments.product_type_l4,
            segments.product_type_l5,
            segments.conversion_action_name,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.conversions,
            metrics.conversions_value
            ]
            
            shopping_conversions = shopping_conversions.append(pd.Series(df_entry, index = shopping_conversions_columns), ignore_index = True)
    for i in shopping_conversions.columns:
        if 'object' in str(type(shopping_conversions[i].dtype)):
            shopping_conversions[i] = shopping_conversions[i].apply(lambda x: str(x))
    shopping_conversions[['Customer_ID', 'Campaign_ID','Ad_Group_ID']] = shopping_conversions[[ 'Customer_ID', 'Campaign_ID','Ad_Group_ID']].astype(float)
    shopping_conversions['Conversion_Action'] = shopping_conversions['Conversion_Action'].str.lower().apply(lambda x: re.sub('[^A-Za-z0-9_\u4e00-\u9fff]+', '_', x))
    for i in set(shopping_conversions['Advertiser']):    
        key = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type',
        'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Product_Type_1','Product_Type_2', 'Product_Type_3', 'Product_Type_4', 'Product_Type_5', 'Conversion_Action']
        shopping_conversions = shopping_conversions.groupby(by = key).sum()
        shopping_conversions = pd.DataFrame(shopping_conversions.to_records())
        #Ad Group Ad with keyword
        key = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_Name', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type',
        'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Product_Type_1','Product_Type_2', 'Product_Type_3', 'Product_Type_4', 'Product_Type_5']

        pre_Campaign_Conversion = pd.pivot_table(shopping_conversions[shopping_conversions['Advertiser'] == i], index=key, columns='Conversion_Action',
                                                values = [i for i in shopping_conversions.columns if i != 'Conversion_Action' and i not in key],
                                                aggfunc=np.sum, fill_value=0)
        pre_Campaign_Conversion = pre_Campaign_Conversion.groupby(key).sum()
        pre_Campaign_Conversion = pd.DataFrame(pre_Campaign_Conversion.to_records())

        shopping_conversion_columns = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                shopping_conversion_columns.append([j])
            else:
                shopping_conversion_columns.append(j)
        shopping_conversion_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in shopping_conversion_columns]
        pre_Campaign_Conversion.columns = shopping_conversion_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        pre_Campaign_Conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        try: 
            sql = f"""
                SELECT * FROM {name}.Shopping_Ad_Conversion
                WHERE Date < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Shopping_Ad_Conversion')
        except: 
            DATA = pre_Campaign_Conversion
            
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Shopping_Ad_Conversion')


        pre_Campaign_Conversion = shopping_summary[shopping_summary['Advertiser'] == i].merge(pre_Campaign_Conversion, how = 'left', on = key)

        ad_group_conv_action_columns = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                ad_group_conv_action_columns.append([j])
            else:
                ad_group_conv_action_columns.append(j)
        ad_group_conv_action_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in ad_group_conv_action_columns]
        pre_Campaign_Conversion.columns = ad_group_conv_action_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)

        pre_Campaign_Conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        
        try: 
            sql = f"""
                SELECT * FROM {name}.Shopping_Ad_Merged
                WHERE Date < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Shopping_Ad_Merged')
            
        except: 
            DATA = pre_Campaign_Conversion
            job_config = bigquery.LoadJobConfig(
                    
                    )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Shopping_Ad_Merged')

def getShoppingSummaryReport(googleads_client, client_id):
    query = f"""
    SELECT
    segments.date,
    customer.descriptive_name,
    customer.id,
    campaign.name,
    campaign.id,
    ad_group.labels,
    campaign.labels,
    campaign.advertising_channel_sub_type,
    ad_group.type,
    ad_group.name,
    ad_group.id,
    segments.product_title,
    segments.product_type_l1,
    segments.product_type_l2,
    segments.product_type_l3,
    segments.product_type_l4,
    segments.product_type_l5,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.clicks,
    metrics.conversions,
    metrics.conversions_value,
    metrics.cost_micros,
    metrics.impressions
    FROM shopping_performance_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """
    shopping_summary_columns = ['Date', 'Advertiser', 'Customer_ID','Campaign_Name', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Channel_Sub_Type', 'Ad_Group_Type', 'Ad_Group_ID', 'Ad_Group_Name', 'Product_Title', 'Product_Type_1', 
    'Product_Type_2', 'Product_Type_3', 'Product_Type_4', 'Product_Type_5', 'All_Conversions','All_Conversions_Value', 'Clicks', 'Conversions', 'Conversions_Value', 'Cost', 'Impressions']


    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    
    shopping_summary = pd.DataFrame(columns = shopping_summary_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            ad_group = row.ad_group
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            df_entry = [
            segments.date,
            customer.descriptive_name,
            customer.id,
            campaign.name,
            campaign.id,
            ad_group.labels,
            campaign.labels,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.type_.name,
            ad_group.id,
            ad_group.name,
            segments.product_title,
            segments.product_type_l1,
            segments.product_type_l2,
            segments.product_type_l3,
            segments.product_type_l4,
            segments.product_type_l5,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_micros/1000000,
            metrics.impressions
            ]
            
            shopping_summary = shopping_summary.append(pd.Series(df_entry, index = shopping_summary_columns), ignore_index = True)

    for i in shopping_summary.columns:
            if 'object' in str(type(shopping_summary[i].dtype)):
                shopping_summary[i] = shopping_summary[i].apply(lambda x: str(x))
                
    for i in set(shopping_summary['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        
        Q4 = f"""
        DELETE FROM {name}.Shopping_Ad_Summary
        WHERE Date BETWEEN '{start}' AND '{end}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)     
        
        
        shopping_summary[['Customer_ID', 'Campaign_ID','Ad_Group_ID']] = shopping_summary[[ 'Customer_ID', 'Campaign_ID','Ad_Group_ID']].astype(float)
        shopping_summary[['Impressions','Clicks']] = shopping_summary[['Impressions','Clicks']].astype(int)
        job_config = bigquery.LoadJobConfig(
            
        )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

        bq_client.load_table_from_dataframe(dataframe = shopping_summary[shopping_summary['Advertiser'] == i], job_config = job_config, destination = f'{name}.Shopping_Ad_Summary')

    return shopping_summary

def getAdGroupConversionReport(googleads_client, client_id, ad_group_summary):
    
    query = f"""
    SELECT
    segments.date,
    customer.descriptive_name, 
    customer.id, 
    campaign.id,
    ad_group.labels,
    campaign.labels,
    campaign.name,
    campaign.advertising_channel_sub_type,
    ad_group.type,
    ad_group.name,
    ad_group.id,
    segments.week,
    segments.month,
    segments.conversion_action_name,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.conversions,
    metrics.conversions_value
    FROM ad_group
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """


    ad_group_conversion_columns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Name', 'Campaign_Channel_Sub_Type', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Week', 'Month', 'Conversion_Action', 'All_Conversions', 'All_Conversions_Value', 'Conversions', 'Conversions_Value']

    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )

    
    ad_group_conversion = pd.DataFrame(columns = ad_group_conversion_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            ad_group = row.ad_group
            df_entry = [  
            segments.date,
            customer.descriptive_name, 
            customer.id, 
            campaign.id,
            ad_group.labels,
            campaign.labels,
            campaign.name,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            ad_group.type_.name,
            ad_group.name,
            ad_group.id,
            segments.week,
            segments.month,
            segments.conversion_action_name,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.conversions,
            metrics.conversions_value
            ]
            
            ad_group_conversion = ad_group_conversion.append(pd.Series(df_entry, index = ad_group_conversion_columns), ignore_index = True)
    for i in ad_group_conversion.columns:
        if 'object' in str(type(ad_group_conversion[i].dtype)):
            ad_group_conversion[i] = ad_group_conversion[i].apply(lambda x: str(x))
        
    ad_group_conversion[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID']] = ad_group_conversion[['Customer_ID', 'Campaign_ID', 'Ad_Group_ID']].astype(float)
    ad_group_conversion['Conversion_Action'] = ad_group_conversion['Conversion_Action'].str.lower().apply(lambda x: re.sub('[^A-Za-z0-9_\u4e00-\u9fff]+', '_', x))
    for i in set(ad_group_conversion['Advertiser']):    
        key = ['Date', 'Advertiser', 'Campaign_Channel_Sub_Type', 'Customer_ID', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Ad_Group_ID', 'Ad_Group_Name', 'Campaign_Name', 'Conversion_Action']
        ad_group_conversion = ad_group_conversion.groupby(key).sum()
        ad_group_conversion = pd.DataFrame(ad_group_conversion.to_records())
        key = ['Date', 'Advertiser', 'Campaign_Channel_Sub_Type', 'Customer_ID', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Ad_Group_ID', 'Ad_Group_Name', 'Campaign_Name']
        #Ad Group Ad with keyword
        pre_Campaign_Conversion = pd.pivot_table(ad_group_conversion[ad_group_conversion['Advertiser'] == i], index=key, columns='Conversion_Action',
                                                values = [i for i in ad_group_conversion.columns if i != 'Conversion_Action' and i not in key],
                                                aggfunc=np.sum, fill_value=0)

        pre_Campaign_Conversion = pre_Campaign_Conversion.groupby(key).sum()

        pre_Campaign_Conversion = pd.DataFrame(pre_Campaign_Conversion.to_records())
        
        
        keyword_ad_group_column = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                keyword_ad_group_column.append([j])
            else:
                keyword_ad_group_column.append(j)
        keyword_ad_group_column = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in keyword_ad_group_column]
        pre_Campaign_Conversion.columns = keyword_ad_group_column
        
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        pre_Campaign_Conversion[['Customer_ID', 'Campaign_ID','Ad_Group_ID']] = pre_Campaign_Conversion[['Customer_ID', 'Campaign_ID','Ad_Group_ID']].astype(float)

        try: 
            sql = f"""
                SELECT * FROM {name}.Ad_Group_Conversion
                WHERE Date < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            job_config = bigquery.LoadJobConfig(
                
                        )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Conversion')
        except: 
            DATA = pre_Campaign_Conversion
            job_config = bigquery.LoadJobConfig(
                
                        )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Conversion')


        

        pre_Campaign_Conversion = ad_group_summary[ad_group_summary['Advertiser'] == i].merge(pre_Campaign_Conversion, how = 'left', on = key)

        ad_group_conv_action_columns = []
        for j in pre_Campaign_Conversion.columns:
            j = j.split(',')   
            if(len(j) > 1):
                j = j[1][0]+ j[1][1:].replace(' ', '_') + '_' + j[0]
                ad_group_conv_action_columns.append([j])
            else:
                ad_group_conv_action_columns.append(j)
        ad_group_conv_action_columns = [re.sub(r'[^a-zA-Z_0-9]','',k[0]) for k in ad_group_conv_action_columns]
        pre_Campaign_Conversion.columns = ad_group_conv_action_columns
        name = re.sub(r'[^a-zA-Z_0-9]','',i)

        pre_Campaign_Conversion = pre_Campaign_Conversion.replace(np.nan, 0)
        
        try: 
            sql = f"""
                SELECT * FROM {name}.Ad_Group_Merged
                WHERE Date < '{start}'
            """
            hist_df = pd.read_gbq(sql, project_id= 'go0gle-ads', dialect = 'standard', credentials = gbq_creds, use_bqstorage_api=True)
            DATA = pre_Campaign_Conversion
            DATA = DATA.append(hist_df, ignore_index = True)
            job_config = bigquery.LoadJobConfig(
                
                        )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Merged')
        except: 
            DATA = pre_Campaign_Conversion
            job_config = bigquery.LoadJobConfig(
                
                        )
            job_config.write_disposition = "WRITE_TRUNCATE"
            bq_client.load_table_from_dataframe(dataframe = DATA, job_config = job_config, destination = f'{name}.Ad_Group_Merged')

def getAdGroupSummaryReport(googleads_client, client_id):
    query = f"""
    SELECT
    segments.date,
    customer.descriptive_name, 
    customer.id, 
    campaign.id,
    ad_group.labels,
    campaign.labels,
    campaign.name,
    campaign.advertising_channel_sub_type,
    ad_group.type,
    ad_group.name,
    ad_group.id,
    segments.week,
    segments.month,
    metrics.absolute_top_impression_percentage,
    metrics.active_view_impressions,
    metrics.active_view_measurability,
    metrics.active_view_measurable_impressions,
    metrics.active_view_viewability,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.clicks,
    metrics.content_impression_share,
    metrics.content_rank_lost_impression_share,
    metrics.conversions,
    metrics.conversions_value,
    metrics.cost_micros,
    metrics.current_model_attributed_conversions,
    metrics.current_model_attributed_conversions_value,
    metrics.engagements,
    metrics.impressions,
    metrics.interaction_event_types,
    metrics.interactions,
    metrics.search_absolute_top_impression_share,
    metrics.search_budget_lost_absolute_top_impression_share,
    metrics.search_budget_lost_top_impression_share,
    metrics.search_exact_match_impression_share,
    metrics.search_impression_share,
    metrics.search_rank_lost_absolute_top_impression_share,
    metrics.search_rank_lost_impression_share,
    metrics.search_rank_lost_top_impression_share,
    metrics.search_top_impression_share,
    metrics.top_impression_percentage,
    metrics.video_quartile_p100_rate,
    metrics.video_quartile_p25_rate,
    metrics.video_quartile_p50_rate,
    metrics.video_quartile_p75_rate,
    metrics.video_view_rate,
    metrics.video_views,
    metrics.view_through_conversions
    FROM ad_group
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    """


    ad_group_summary_columns = ['Date', 'Advertiser', 'Customer_ID', 'Campaign_ID', 'Ad_Group_Labels', 'Campaign_Labels', 'Campaign_Name', 'Ad_Group_Type', 'Ad_Group_Name', 'Ad_Group_ID', 'Campaign_Channel_Sub_Type',
    'Week', 'Month', 'Absolute_Top_Impression_Percentage', 'Active_View_Impressions', 'Active_View_Measurability', 'Active_View_Measurable_Impressions',
    'Active_View_Viewability', 'All_Conversions', 'All_Conversions_Value', 'Clicks', 'Content_Impression_Share',
    'Content_Rank_Lost_Impression_Share', 'Conversions', 'Conversions_Value', 'Cost', 'Current_Model_Attributed_Conversions', 
    'Current_Model_Attributed_Conversions_Value', 'Engagements', 'Impressions', 'Interaction_Event_Types', 'Interactions', 'Search_Absolute_Top_Impression_Share',
    'Search_Budget_Lost_Absolute_Top_Impression_Share', 'Search_Budget_Lost_Top_Impression_Share', 
    'Search_Exact_Match_Impression_Share', 'Search_Impression_Share', 'Search_Rank_Lost_Absolute_Top_Impression_Share',
    'Search_Rank_Lost_Impression_Share', 'Search_Rank_Lost_Top_Impression_Share', 'Search_Top_Impression_Share', 'Top_Impression_Percentage', 'Video_Quartile_p25_rate', 'Video_Quartile_p50_rate', 'Video_Quartile_p75_rate', 'Video_Quartile_p100_rate', 
    'Video_View_Rate', 'Video_Views', 'View_Through_Conversions'] 

    
    while True:
        try:
            ga_service = googleads_client.get_service("GoogleAdsService")
            stream = ga_service.search_stream(customer_id=client_id, query=query)
            break
        except Exception as ex:
                # This example retries on all GoogleAdsExceptions. In practice,
                # developers might want to limit retries to only those error codes
                # they deem retriable.
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    time.sleep(retry_count * BACKOFF_FACTOR)
                else:
                    return (
                        'Error',
                        {
                            "exception": ex,
                            "customer_id": client_id
                        },
                     )


    ad_group_summary = pd.DataFrame(columns = ad_group_summary_columns)
    for batch in stream:
        for row in batch.results:
            segments = row.segments
            customer = row.customer
            metrics = row.metrics
            campaign = row.campaign
            ad_group = row.ad_group
            df_entry = [
            segments.date,
            customer.descriptive_name, 
            customer.id, 
            campaign.id,
            ad_group.labels,
            campaign.labels,
            campaign.name,
            ad_group.type_.name,
            ad_group.name,
            ad_group.id,
            vars(campaign.advertising_channel_sub_type)['_name_'],
            segments.week,
            segments.month,
            metrics.absolute_top_impression_percentage,
            metrics.active_view_impressions,
            metrics.active_view_measurability,
            metrics.active_view_measurable_impressions,
            metrics.active_view_viewability,
            metrics.all_conversions,
            metrics.all_conversions_value,
            metrics.clicks,
            metrics.content_impression_share,
            metrics.content_rank_lost_impression_share,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_micros/1000000,
            metrics.current_model_attributed_conversions,
            metrics.current_model_attributed_conversions_value,
            metrics.engagements,
            metrics.impressions,
            vars(metrics.interaction_event_types[0])['_name_'] if len(metrics.interaction_event_types) > 0 else '',
            metrics.interactions,
            metrics.search_absolute_top_impression_share,
            metrics.search_budget_lost_absolute_top_impression_share,
            metrics.search_budget_lost_top_impression_share,
            metrics.search_exact_match_impression_share,
            metrics.search_impression_share,
            metrics.search_rank_lost_absolute_top_impression_share,
            metrics.search_rank_lost_impression_share,
            metrics.search_rank_lost_top_impression_share,
            metrics.search_top_impression_share,
            metrics.top_impression_percentage,
            metrics.video_quartile_p25_rate,
            metrics.video_quartile_p50_rate,
            metrics.video_quartile_p75_rate,
            metrics.video_quartile_p100_rate,
            metrics.video_view_rate,
            metrics.video_views,
            metrics.view_through_conversions
            ]
            
            ad_group_summary = ad_group_summary.append(pd.Series(df_entry, index = ad_group_summary_columns), ignore_index = True)
    
    
    for i in ad_group_summary.columns:
        if 'object' in str(type(ad_group_summary[i].dtype)):
            ad_group_summary[i] = ad_group_summary[i].apply(lambda x: str(x))
    
    ad_group_summary['Campaign_Channel_Sub_Type'] = ad_group_summary['Campaign_Channel_Sub_Type'].astype(str)
    ad_group_summary[[ 'Impressions','Clicks','Active_View_Impressions', 'Active_View_Measurable_Impressions', 'Engagements', 'Interactions', 'Video_Views', 'View_Through_Conversions']] = ad_group_summary[['Impressions','Clicks', 'Active_View_Impressions', 'Active_View_Measurable_Impressions', 'Engagements', 'Interactions', 'Video_Views', 'View_Through_Conversions']].astype(int)
    ad_group_summary[[ 'Cost', 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID']] = ad_group_summary[['Cost', 'Customer_ID', 'Campaign_ID', 'Ad_Group_ID']].astype(float)
    for i in set(ad_group_summary['Advertiser']):
        name = re.sub(r'[^a-zA-Z_0-9]','',i)
        try:
            bq_client.get_dataset(name)  # Make an API request.
        except NotFound:
            project_id = bq_client.project
            dataset = bigquery.Dataset(f'{project_id}.{name}')
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset, timeout=30)
            print(f'dataset {name} created')

        Q4 = f"""
        DELETE FROM {name}.Ad_Group_Summary
        WHERE Date BETWEEN '{start}' AND '{end}'
            """

        # run the query
        query_job4 = bq_client.query(Q4)     
        job_config = bigquery.LoadJobConfig(
            
                )
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

        bq_client.load_table_from_dataframe(dataframe = ad_group_summary[ad_group_summary['Advertiser'] == i], job_config = job_config, destination = f'{name}.Ad_Group_Summary')
        
    return ad_group_summary

def main(client, customer_ids):
    """The main method that creates all necessary entities for the example.
    Args:
        client: an initialized GoogleAdsClient instance.
        customer_ids: an array of client customer IDs.
    """

    # Define the GAQL query strings to run for each customer ID.
    
    #generate inputs and set inputs variable to it. 
    
    # Retry until we've reached MAX_RETRIES or have successfully received a
    # response.
    BACKOFF_FACTOR = 5
    MAX_RETRIES = 5
    retry_count = 0
    
    
    while True:
        try:
            print('ad group summary')
            Ad_Group_Ad_Summary = getAdGroupAdSummaryReport(client, customer_ids)
            print('passed')
            getAdGroupAdConversionReport(client, customer_ids, Ad_Group_Ad_Summary)
            keyword_summary = getKeywordSummaryReport(client, customer_ids)
            getKeywordConversionReport(client, customer_ids, keyword_summary)
            search_term_summary = getSearchTermSummaryReport(client, customer_ids)
            getSearchTermConversionReport(client, customer_ids, search_term_summary)
            shopping_summary = getShoppingSummaryReport(client, customer_ids)
            getShoppingConversionReport(client, customer_ids, shopping_summary)
            ad_group_summary = getAdGroupSummaryReport(client, customer_ids)
            getAdGroupConversionReport(client, customer_ids, ad_group_summary)
            getImpressionShareReport(client, customer_ids)
            getShoppingCampaignReport(client, customer_ids)
            getCampaignMergeData(client, customer_ids)
            #functions that run queries and also enter into GBQ the data, passing summary results to conversion functions.
            
            
            return 'Finished'
        except GoogleAdsException as ex:
            # This example retries on all GoogleAdsExceptions. In practice,
            # developers might want to limit retries to only those error codes
            # they deem retriable.
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                return (
                    'Error',
                    {
                        "exception": ex,
                        "customer_id": client_id
                    },
                )

if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    BACKOFF_FACTOR = 5
    MAX_RETRIES = 5
    retry_count = 0
    #client_id = ['12342323', '12312312313', '123123121231']
    while True:
        try:
            googleads_client = GoogleAdsClient.load_from_storage(path = 'google-ads.yaml', version="v9")
            break
        except GoogleAdsException as ex:
            # This example retries on all GoogleAdsExceptions. In practice,
            # developers might want to limit retries to only those error codes
            # they deem retriable.
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                print(
                    f"""'Error',
                    {
                        "exception": {ex},
                        "customer_id": {client_id}
                    }
                    """
                )
                break
        
    for i in client_id:
            print(i)
            main(googleads_client, i)

