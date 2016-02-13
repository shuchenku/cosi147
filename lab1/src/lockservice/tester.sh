#!/bin/bash
for i in {1..300}
do
	go test >> repeat_test_output;
	sleep 1
done;