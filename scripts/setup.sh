#!/bin/sh

# This file is part of VoltDB.
#  Copyright (C) 2008-2024 VoltDB Inc.
#
#  Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
#  "Software"), to deal in the Software without restriction, including
#  without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so, subject to
#  the following conditions:
#
#  The above copyright notice and this permission notice shall be
#  included in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
#  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
#  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
#  OTHER DEALINGS IN THE SOFTWARE.


if
	[ -r  "$HOME/.profile" ]
then
	. $HOME/.profile
fi

#USERCOUNT=4000000

THISDIR=`pwd`
VOLTHOST=localhost

if
	[ -r "$HOME/.vdbhostnames" ]
then
	VOLTHSOT=`cat $HOME/.vdbhostnames`
fi

#cd 
#mkdir logs 2> /dev/null
#cd voltdb-simple-kafka/ddl
cd ../ddl
sqlcmd --servers=${VOLTHOST} < create_db.sql

java  ${JVMOPTS}  -jar ../jars/addtodeploymentdotxml.jar ${VOLTHOST}  deployment ../scripts/export_and_import.xml

cd /Users/dwrolfe/Downloads/kafka_2.13-2.6.0/bin
sh kafka-topics.sh --bootstrap-server localhost:9092 --list
sh kafka-console-producer.sh --bootstrap-server localhost:9092 --topic UPSERT_DRIVERS  < ${THISDIR}/drivers.csv
sh kafka-console-producer.sh --bootstrap-server localhost:9092 --topic UPSERT_VEHICLES < ${THISDIR}/vehicles.csv

echo 'joe bloggs','abc123',151,Vroom | sh kafka-console-producer.sh --bootstrap-server localhost:9092 --topic REPORT_USAGE
sleep 10
echo 'joe bloggs','abc123',151,END| sh kafka-console-producer.sh --bootstrap-server localhost:9092 --topic REPORT_USAGE

cd $THISDIR

