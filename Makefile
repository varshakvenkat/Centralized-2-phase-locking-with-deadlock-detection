setup: requirements.txt
	pip3 install -r requirements.txt

run-2pl:
ifdef folder
else
	@echo "No folder specified"
	exit 1
endif
ifdef no_sites
else
	@echo "No no_sites specified"
	exit 1
endif
	sh run.sh $(folder) $(no_sites)

clean:
	rm -rf __pycache__