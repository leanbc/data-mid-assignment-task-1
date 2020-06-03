CURRENT_DIR_NAME:=`pwd | xargs basename`
INTERPRETER_IMAGE_ID=python:3.8

_test_project:
	( \
	set -e; \
	pip3 install -r requirements.txt; \
	pytest -vv; \
	)
	
docker_run_tests:
	( \
	docker run -i -w /$(CURRENT_DIR_NAME) -v `pwd`:/$(CURRENT_DIR_NAME) $(INTERPRETER_IMAGE_ID) \
	make _test_project; \
	)
