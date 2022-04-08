

CREATE TABLE
  advertising-analytics-grp5.Ad_enriching.Page(
	publisher_id INT64 ,
	page_id STRING,
	publisher STRING,
	article STRING );

CREATE TABLE
  advertising-analytics-grp5.Ad_enriching.Advertiser(
	ad_id INT64,
	advertiser STRING,
	exchange STRING,
	compaign_id INT64,
	size STRING );
 
CREATE TABLE
  advertising-analytics-grp5.Ad_enriching.Ad_position(
	ad_position STRING,
	Location STRING,
	Size STRING );
 
CREATE TABLE
  advertising-analytics-grp5.Ad_enriching.Browser(
	user_agent STRING,
	browser STRING,
	version NUMERIC );
 
CREATE TABLE Ad_enriching.WebUsers (
    user_id STRING, 
    Age INT64, 
    Country STRING, 
    GENDER STRING, 
    MARITAL_STATUS STRING, 
    INCOME INT64);


CREATE TABLE Ad_analytics.Ad_impressions (
    ad_id INT64,
    user_id STRING,
    publisher_id INT64,
    page_id STRING,
    ad_position STRING,
    latency INT64,
    timestamp TIMESTAMP,
    clicked STRING,
    Location STRING,
    Size STRING, 
    advertiser STRING, 
    exchange STRING,
    campaign_id INT64,
    publisher STRING,
    article STRING, 
    Browser STRING,
    Browser_version STRING,
    Client_OS STRING, 
    Client_OS_Ver STRING, 
    Client_Device STRING, 
    Client_Device_Brand STRING,
    Client_Device_Model STRING);



CREATE TABLE Ad_analytics.Ad_impressions_Invalid(
    ad_id INT64,
    user_id STRING,
    publisher_id INT64,
    page_id STRING,
    ad_position STRING,
    latency INT64,
    timestamp TIMESTAMP,
    user_agent STRING,
    clicked STRING,
    validity_status STRING,
    invalid_field STRING);


INSERT INTO advertising-analytics-grp5.Ad_enriching.WebUsers
select a.user_id,37,'USA','MALE','MARRIED',50000 from
(select distinct user_id from advertising-analytics-grp5.Ad_analytics.Ad_impressions) a
left join advertising-analytics-grp5.Ad_enriching.WebUsers u on a.user_id=u.user_id
where u.user_id is NULL LIMIT 50
;

