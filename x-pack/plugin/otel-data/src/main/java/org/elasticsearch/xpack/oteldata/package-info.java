/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * This package contains index templates for OpenTelemetry data. It covers traces (spans), metrics, and logs.
 * The plugin is expected to be used in combination with the Elasticsearch exporter defined as the exporter
 * within an OpenTelemetry collector with the mapping mode `otel`.
 * For more information about the Elasticsearch exporter
 * @see <a href="https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/elasticsearchexporter/README.md">
 * https://github.com/open-telemetry/opentelemetry-collector-contrib</a>.
 *
 */
package org.elasticsearch.xpack.oteldata;
