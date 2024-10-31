VERSION := $(shell cat pyproject.toml | grep '^version\>' | sed 's/^version\s=\s*"\([^"]\+\)"/\1/')

DOCKER_TAG = barskern/strom:$(VERSION)

build:
	docker build -t $(DOCKER_TAG) .
.PHONY: build

deploy:
	docker push $(DOCKER_TAG)
.PHONY: deploy

