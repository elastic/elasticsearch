/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

/**
 * Who sent an ES|QL query, as far as the request tells. Every component is a closed, low-cardinality label: raw header values
 * never reach a counter or an APM attribute. Built by {@link QueryCallerClassifier}; more components (for example the Kibana
 * feature, or the direct client) belong here so every telemetry feed reads them from one place.
 */
public record QueryCaller(QueryMetric client) {}
