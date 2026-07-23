
.PHONY: help build-libraries publish-libraries

# See https://www.thapaliya.com/en/writings/well-documented-makefiles/
help:
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make <target>\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-20s %s\n", $$1, $$2 } /^##@/ { printf "\n%s\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

build-libraries: ## Build native libraries (requires Docker)
	build-tools/build-libraries.sh

publish-libraries: ## Build and publish libraries (requires Docker)
	build-tools/publish-libraries.sh
