
PROJECT?=$(shell pwd)
HOME?=$(shell echo ~)


produce_18M_csv_file:
	for i in $(seq 1 10000); do cat ./data/1800.csv >> ./data/1800-ml.csv; done
	@echo "$@ finished!"

upload_18M_csv_file:
	gsutil cp ./data/1800-ml.csv gs://intuit-sandbox-datalake/test_dataset/ml1800/*
	@echo "$@ finished!"

%:
	@:
