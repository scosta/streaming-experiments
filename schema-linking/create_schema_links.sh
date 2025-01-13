#!/bin/bash

# Create schema link from sandbox to dev
confluent schema-registry exporter create sandbox-to-dev-exporter --subjects ":*:" --config config1.txt

# Create schema link from sandbox to prod
confluent schema-registry exporter create sandbox-to-prod-exporter --subjects ":*:" --config config2.txt