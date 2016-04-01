#!/bin/bash
for i in {1..10}
	do
		go test >> repeat_test_output;
done;