#!/bin/sh

SRCDIR=`dirname $0`;

# FIXME this should be customizable
n_workers=4;

# copy default configuration file to working directory
#cp ${SRCDIR}/config .;

# copy control scripts to working directory
#cp ${SRCDIR}/start .;
#chmod +x start;

# set up control directories
for d in IN RUNNING DONE FAILED task_log workers; do
    test -d $d || mkdir $d;
done

# set up workers
for i in `seq 1 $n_workers`; do
    mkdir workers/$i;
    mkdir RUNNING/$i;
    cp ${SRCDIR}/worker workers/$i/run;
    chmod +x workers/$i/run;
done