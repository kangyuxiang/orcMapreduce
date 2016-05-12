#!/bin/sh

reduceNumber=2
l_workpath=$(cd $(dirname $0)/;pwd)
l_lib=${l_workpath}/target
plat=$1
timestr=$2
if [[ ${timestr} == "" ]]
then
    timestr=`date -d "-1 day" +"%Y/%m/%d"`
fi

if [ $plat == "pc" ];then
    h_SessionlogInput=/user/bd-warehouse/sessionlog-rc/pc/$timestr
    h_SessionlogOutput=/user/bd-warehouse/mydir/pzf/sessionlog-rc/pc/output
elif [ $plat == "mobile" ];then
    h_SessionlogInput=/user/bd-warehouse/mydir/pzf/sessionlog-rc/mobile/$timestr
    h_SessionlogOutput=/user/bd-warehouse/mydir/pzf/sessionlog-rc/mobile/output
elif [ $plat == "h5" ];then
    h_SessionlogInput=/user/bd-warehouse/sessionlog-rc/h5/$timestr
    h_SessionlogOutput=/user/bd-warehouse/mydir/pzf/sessionlog-rc/h5/output
fi

hadoop fs -rm -r ${h_SessionlogOutput}/${timestr}

hadoop jar ${l_lib}/sessionlogConsumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.pzf.consumer.ConsumerTask \
-D mapreduce.child.java.opts=-Xmx2048m \
-D mapreduce.output.fileoutputformat.compress=com.hadoop.compression.lzo.LzoCodec \
-D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.job.queuename=datacenter \
-D mapreduce.input.fileinputformat.inputdir=${h_SessionlogInput} \
-D mapreduce.output.fileoutputformat.outputdir=${h_SessionlogOutput}/${timestr} \
-D mapreduce.job.reduces=${reduceNumber} \
-D mapreduce.job.name=sessionlog-rc-$plat-$timestr \
-D plat=$plat \
-D column=tvdspclick \
-D field=supplyId
hdfs dfs -setfacl -R -m mask::rwx ${h_SessionlogOutput}/${timestr}

