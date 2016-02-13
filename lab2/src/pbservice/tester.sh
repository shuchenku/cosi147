#!/bin/bash
for i in {1..50}
do
	go test >> repeat_test_output_2;
	sleep 1
done;