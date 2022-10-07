#!/usr/bin/env bash
failed_times=0
total_time=0

for i in $(seq 1 $2)
do
    echo "---TEST(${i}/$2)---"
    
    startTime_s=`date +%s`
    output=`go test -run $1|grep -B 1 -iE 'FAIL|error'`
    endTime_s=`date +%s`
    perTime=$[ $endTime_s - $startTime_s ]
    if [[ "${output}" != "" ]]
    then
        failed_times=`expr $failed_times + 1`
        echo $output
    else
        echo "PASS(time costs:${perTime}s)"
    fi
    total_time=`expr $perTime + $total_time`
done

if [ 0 -eq ${failed_times} ]
then
    echo "---PASS ALL---"
else
    echo "---FAIL SOME TESTS(${failed_times})---"
fi

echo "Total time:${total_time}s"