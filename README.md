# volt-simple-kafka
A simple Kafka demo for Volt Active Data

## Scenario

In this demo we are running a car sharing service. We track three things:

### Drivers

For each driver we store a name and the max speed we let them go:

````
CREATE TABLE drivers 
(driver_name varchar(80) not null primary key
,driver_max_speed bigint not null
,driver_max_vehicles_24_hours bigint not null);
````

To upsert a driver send a comma delimited string to the topic UPSERT_DRIVERS 



### Vehicles

For vehicles we store a Vehicle Identification Number ('VIN'), model and color:
````
CREATE TABLE vehicles
(vin         VARCHAR(48) NOT NULL primary key
,model_name varchar(80) not null
,color      varchar(80));
````

To upsert a vehicle send a comma delimited string to the topic UPSERT_DRIVERS 

### Usage

Usage arrives as a comma delimited string sent to the topic REPORT_USAGE, in the format driver, vin, speed,comment.

So a valid message would be:

````
joe bloggs,abc123,151,Vroom
````

Comment is only important at the end of a rental, when it's 'END'.

````
CREATE TABLE driver_car_use
(driver_name varchar(80) not null
,vin         VARCHAR(48) NOT NULL
,start_time  timestamp   not null
,end_time    timestamp   not null
,status      varchar(40) not null
,max_speed   bigint  
,primary key (driver_name,vin,start_time))
USING TTL 5 DAYS ON COLUMN end_time
````

## Installation

1. cd to the scripts directory
2. run setup.sh

## Running

As usage arrives we pass it to (ReportUsage)[serverSrc/ReportUsage.java], which decides what to do with it. Normally we just update driver_car_usage, but there are exceptions:

1. Driver name not in the driver table
2. VIN number not in the vehicle table
3. Driver drove faster than allowed.

In each case these result in a message to the Topic 'naughty_driver_stream'


````
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
````


