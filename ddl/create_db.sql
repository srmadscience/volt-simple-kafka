load classes ../jars/volt-simple-kafka-server.jar;

CREATE TABLE drivers 
(driver_name varchar(80) not null primary key
,driver_max_speed bigint not null
,driver_max_vehicles_24_hours bigint not null);

CREATE TABLE vehicles
(vin         VARCHAR(48) NOT NULL primary key
,model_name varchar(80) not null
,color      varchar(80));

CREATE TABLE driver_car_use
(driver_name varchar(80) not null
,vin         VARCHAR(48) NOT NULL
,start_time  timestamp   not null
,end_time    timestamp   not null
,status      varchar(40) not null
,max_speed   bigint  
,primary key (driver_name,vin,start_time))
USING TTL 5 DAYS ON COLUMN end_time;

CREATE INDEX dcu_ix1 ON driver_car_use(end_time,driver_name);

PARTITION TABLE driver_car_use ON COLUMN driver_name;

CREATE VIEW driver_activity_hour
AS 
select  driver_name, TRUNCATE(hour, end_time) end_time, max(max_speed) max_speed, count(*) how_many
from driver_car_use
group by driver_name, TRUNCATE(hour, end_time);

CREATE INDEX dah_ix1 ON driver_activity_hour(driver_name, end_time);

CREATE STREAM naughty_driver_stream 
PARTITION  ON COLUMN driver_name
EXPORT TO TOPIC naughty_driver_topic
 WITH KEY (driver_name) VALUE (driver_name,vin,OBSERVED_DATE,driver_max_speed,drive_max_vehicles_24_hours,message_comment)
(driver_name varchar(80) not null
,VIN VARCHAR(48) NOT NULL
,OBSERVED_DATE TIMESTAMP NOT NULL
,driver_max_speed bigint not null
,drive_max_vehicles_24_hours bigint not null
,message_comment  varchar(80));

CREATE PROCEDURE 
   PARTITION ON TABLE driver_car_use COLUMN driver_name
   FROM CLASS ReportUsage;
