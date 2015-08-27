/*
    Params
    ------
    statsday = stats day (also day of messages, and active user file)

*/

SET pig.name 'Process Daily Messages: get messaging stats and active users';

SET default_parallel 10;

REGISTER s3://voxer-analytics/pig/external-udfs/Pigitos-1.0-SNAPSHOT.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/datafu-0.0.5.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/json-simple-1.1.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/guava-11.0.2.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/elephant-bird-core-3.0.0.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/elephant-bird-pig-3.0.0.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/oink.jar;

DEFINE jsonStorage com.voxer.data.pig.store.JsonStorage();
DEFINE timestampToDay com.voxer.data.pig.eval.TimestampConverter('yyyyMMdd');
DEFINE timestampToDOW com.voxer.data.pig.eval.TimestampConverter('E');
DEFINE timestampToHOD com.voxer.data.pig.eval.TimestampConverter('H');
DEFINE domainFilter com.voxer.data.pig.eval.EmailDomainFilter();
DEFINE datestampToMillis com.voxer.data.pig.eval.DatestampConverter('yyyyMMdd','GMT');

DEFINE DistinctBy datafu.pig.bags.DistinctBy('0');

IMPORT 's3://voxer-analytics/pig/macros/aws-load-profiles-raw.pig';
IMPORT 's3://voxer-analytics/pig/macros/aws-load-active-users.pig';

-- ==============================================================
--
-- step 1:
--
-- read the messages and isolate SYSTEM-generated
--


all_messages = LOAD 's3://voxer-flume/prod/$statsday*/messages*' USING jsonStorage as (json:map[]);

all_messages = FOREACH all_messages GENERATE json#'from' as user_id:chararray, 1 as allon:int,
    (json#'message_id' is NULL ? 'no_message_id' : json#'message_id') as message_id:chararray,
    (json#'thread_id' is NULL ? 'no_thread_id' : json#'thread_id') as thread_id:chararray,
    -- json#'geo'#'longitude' as longitude:chararray,
    -- json#'geo'#'latitude' as latitude:chararray,
    (json#'revox' IS NOT NULL ? '"' : json#'geo'#'longitude') as longitude:chararray,
    (json#'revox' IS NOT NULL ? '"' : json#'geo'#'latitude') as latitude:chararray,
    ((double)json#'normalized_create_time') as normal_timestamp:double,
    ((double)json#'posted_time') as posted_timestamp:double,
    -- (int)((((double)json#'normalized_create_time')*1000.0 - datestampToMillis('$statsday')) / (3600.0*1000.0)) as msg_hour:int,
    -- timestampToDOW((double)json#'posted_time') as msg_day:chararray,
    timestampToHOD((double)json#'posted_time') as msg_hour:int,
    timestampToDOW((double)json#'posted_time') as msg_day:chararray,
    (json#'from' == 'voxer.bot@voxer.com' OR json#'from' == 'voxer.bot2@voxer.com' ? 1 : 0) as is_voxerbot:int,
    (json#'sub_content_type' is NULL ? 0 : 1) as has_sub_content_type:int,
    (json#'sub_content_type' is NULL ? 'no_sub_content_type' : json#'sub_content_type') as sub_content_type:chararray,
    (json#'system_generated' is NULL ? 0 : 1) as is_system_generated:int,
    (json#'people_match' is NULL ? 0 : 1) as is_people_match:int,
    (json#'content_type' == 'text' ? 1 : 0) as is_text:int,
    (json#'content_type' == 'audio' ? 1 : 0) as is_audio:int,
    (json#'content_type' == 'image' ? 1 : 0) as is_image:int,
    json#'content_type' as content_type:chararray,
    (json#'audio_duration_ms' is NOT NULL AND json#'audio_duration_ms' > 0.0 ? (double)json#'audio_duration_ms' : 0.0) as audio_duration_ms:double,
    (json#'duration_ms' is NULL ? 0.0 : (double)json#'duration_ms') as duration_ms:double,
    (json#'duration_ms' is NULL ? 0 : 1) as has_duration_ms:int,
    (long)SIZE((bag{})json#'to') as n_peeps:long,
    --grab the system-generated value in order to trap for "reactivate"
    json#'system_generated' AS system_generated:chararray,
    json#'to' as recipients:bag{},
    (json#'model' is NULL ? 'no_model' : json#'model') as model:chararray,
    json#'client_version' as cli_ver:chararray,
    (json#'system_name' is NULL ? 'no_sys_name' : LOWER(json#'system_name')) as sys_name:chararray;




all_messages = FILTER all_messages BY system_generated == 'reactivate';

temp = FOREACH all_messages GENERATE FLATTEN(recipients) AS to_id:chararray, posted_timestamp AS posted_timestamp:double;

-- COUNT DISTINCT recipients for later use in AB testing

temp_ids = FOREACH temp GENERATE to_id;

temp_ids = DISTINCT temp_ids;

temp_ids_group = GROUP temp_ids ALL;

stimulator_sent_count = FOREACH temp_ids_group GENERATE (long)COUNT(temp_ids) as stimulator_ids_count;


--all_messages_for_week = LOAD 's3://voxer-flume/prod/{20141124,20141125,20141126,20141127,20141128,20141129,20141130}*/messages*' USING jsonStorage as (json:map[]);

all_messages_for_week = LOAD 's3://voxer-flume/prod/$statsday*/messages*' USING jsonStorage as (json:map[]);



all_messages_for_week = FOREACH all_messages_for_week GENERATE json#'from' as user_id:chararray, 1 as allon:int,
    (json#'message_id' is NULL ? 'no_message_id' : json#'message_id') as message_id:chararray,
    (json#'thread_id' is NULL ? 'no_thread_id' : json#'thread_id') as thread_id:chararray,
    -- json#'geo'#'longitude' as longitude:chararray,
    -- json#'geo'#'latitude' as latitude:chararray,
    (json#'revox' IS NOT NULL ? '"' : json#'geo'#'longitude') as longitude:chararray,
    (json#'revox' IS NOT NULL ? '"' : json#'geo'#'latitude') as latitude:chararray,
    ((double)json#'normalized_create_time') as normal_timestamp:double,
    ((double)json#'posted_time') as posted_timestamp:double,
    -- (int)((((double)json#'normalized_create_time')*1000.0 - datestampToMillis('$statsday')) / (3600.0*1000.0)) as msg_hour:int,
    -- timestampToDOW((double)json#'posted_time') as msg_day:chararray,
    timestampToHOD((double)json#'posted_time') as msg_hour:int,
    timestampToDOW((double)json#'posted_time') as msg_day:chararray,
    (json#'from' == 'voxer.bot@voxer.com' OR json#'from' == 'voxer.bot2@voxer.com' ? 1 : 0) as is_voxerbot:int,
    (json#'sub_content_type' is NULL ? 0 : 1) as has_sub_content_type:int,
    (json#'sub_content_type' is NULL ? 'no_sub_content_type' : json#'sub_content_type') as sub_content_type:chararray,
    (json#'system_generated' is NULL ? 0 : 1) as is_system_generated:int,
    (json#'people_match' is NULL ? 0 : 1) as is_people_match:int,
    (json#'content_type' == 'text' ? 1 : 0) as is_text:int,
    (json#'content_type' == 'audio' ? 1 : 0) as is_audio:int,
    (json#'content_type' == 'image' ? 1 : 0) as is_image:int,
    json#'content_type' as content_type:chararray,
    (json#'audio_duration_ms' is NOT NULL AND json#'audio_duration_ms' > 0.0 ? (double)json#'audio_duration_ms' : 0.0) as audio_duration_ms:double,
    (json#'duration_ms' is NULL ? 0.0 : (double)json#'duration_ms') as duration_ms:double,
    (json#'duration_ms' is NULL ? 0 : 1) as has_duration_ms:int,
    (long)SIZE((bag{})json#'to') as n_peeps:long,
    --grab the system-generated value in order to trap for "reactivate"
    json#'system_generated' AS system_generated:chararray,
    json#'to' as recipients:bag{},
    (json#'model' is NULL ? 'no_model' : json#'model') as model:chararray,
    json#'client_version' as cli_ver:chararray,
    (json#'system_name' is NULL ? 'no_sys_name' : LOWER(json#'system_name')) as sys_name:chararray;


-- Filter FROM messages to isolate USER generated

all_messages_for_week = FILTER all_messages_for_week BY is_voxerbot == 0;

all_messages_for_week = FILTER all_messages_for_week BY is_people_match == 0;

all_messages_for_week = FILTER all_messages_for_week BY is_system_generated == 0;


temp_join = JOIN all_messages_for_week BY user_id, temp BY to_id;

stimulated_message_curve = FOREACH temp_join GENERATE all_messages_for_week::user_id AS user_id:chararray,
                                                    all_messages_for_week::posted_timestamp AS posted_timestamp_from:double,
                                                    temp::posted_timestamp AS posted_timestamp_to:double;


stimulated_message_curve_response = FOREACH stimulated_message_curve GENERATE user_id,
                                                (long)(posted_timestamp_from - posted_timestamp_to) AS response_time;


--rmf s3://voxer-data/ari/growth/stimulated_message_curve

--STORE stimulated_message_curve_response  INTO 's3://voxer-data/ari/growth/stimulated_message_curve' USING PigStorage();


-- Now obtain the control sample of the most recent last_active list

inactive_sample = LOAD 's3://voxer-data/$priorstatsday/last-activity' USING PigStorage('\t');

inactive_sample = FOREACH inactive_sample GENERATE $0 AS user_id, $2 AS last_sent:chararray;


test = FOREACH inactive_sample GENERATE user_id,
                            (long)((datestampToMillis('20141201') - datestampToMillis(last_sent))/(1000*3600*24)) AS last_active_diff;

test = FILTER test BY last_active_diff >= 3.0;

test_group = GROUP test ALL;

test_count = FOREACH test_group GENERATE (long)COUNT(test) AS ids_count;

scrap_cross = CROSS test_count, stimulator_sent_count;

scrap_cross_2 = FOREACH scrap_cross GENERATE (long)(scrap_cross.$0/scrap_cross.$1) as ratio:long;

--STORE scrap_cross_2  INTO 'hdfs://data-01-internal:8020/data/ari/growth/cross_test' USING PigStorage();

test_subsample = SAMPLE test 1.0/scrap_cross_2.ratio;

--STORE test_subsample INTO 'hdfs://data-01-internal:8020/data/ari/growth/control_test_sample' USING PigStorage();


control_sample = FOREACH test_subsample GENERATE user_id, last_active_diff;


--rmf s3://voxer-data/ari/growth/control_sample

--STORE control_sample INTO 's3://voxer-data/ari/growth/control_sample' USING PigStorage();

control_sample_msgs_forweek = JOIN all_messages_for_week BY user_id, control_sample BY user_id;

control_sample_msgs_forweek = FOREACH control_sample_msgs_forweek GENERATE control_sample::user_id AS user_id:chararray,
                                                                control_sample::last_active_diff AS last_active_diff:chararray,
                                                                all_messages_for_week::posted_timestamp AS posted_timestamp_control:double;


--rmf s3://voxer-data/ari/growth/control_message_curve

--STORE control_sample_msgs_forweek INTO 's3://voxer-data/ari/growth/control_message_curve' USING PigStorage();

/*
A/B for the message curve
*/

A_group = GROUP stimulated_message_curve_response ALL;

A = FOREACH A_group GENERATE (long)COUNT(stimulated_message_curve_response) AS A_count;

B_group = GROUP control_sample_msgs_forweek ALL;

B = FOREACH B_group GENERATE (long)COUNT(control_sample_msgs_forweek) AS B_count;

A_B_CROSS = CROSS A, B;

A_B = FOREACH A_B_CROSS GENERATE A_B_CROSS.$0 AS stimulated,
                                A_B_CROSS.$1 AS unstimulated;

--DUMP A_B;

/*
A/B for the user count
*/


stimulated_message_curve_response_users = FOREACH stimulated_message_curve_response GENERATE user_id;

stimulated_message_curve_response_users = DISTINCT stimulated_message_curve_response_users;


alpha_group = GROUP stimulated_message_curve_response_users ALL;

alpha = FOREACH alpha_group GENERATE (long)COUNT(stimulated_message_curve_response_users) as alpha_count;


control_sample_msgs_forweek_users = FOREACH control_sample_msgs_forweek GENERATE user_id;

control_sample_msgs_forweek_users = DISTINCT control_sample_msgs_forweek_users;


beta_group = GROUP control_sample_msgs_forweek_users ALL;

beta = FOREACH beta_group GENERATE (long)COUNT(control_sample_msgs_forweek_users) as beta_count;

alpha_beta_CROSS = CROSS alpha, beta;

alpha_beta = FOREACH alpha_beta_CROSS GENERATE alpha_beta_CROSS.$0 AS stimulated_count,
                                            alpha_beta_CROSS.$1 AS control_count;

--DUMP alpha_beta;

/*
Acummulate all output into one schemea
*/

results_CROSS = CROSS A_B, alpha_beta;

results = FOREACH results_CROSS GENERATE results_CROSS.$0 AS stimulated_msgs,
                                         results_CROSS.$1 AS unstimulated_msgs,
                                         results_CROSS.$2 AS stimulated_users,
                                         results_CROSS.$3 AS unstimulated_users;



STORE results INTO 's3://voxer-data/daily-stats/$statsday.A-B.cumulative' USING PigStorage();

























