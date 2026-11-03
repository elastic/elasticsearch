/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.telemetry.metric.MetricAttributes;

/**
 * Who sent an ES|QL query, a closed set derived by {@link QueryCallerClassifier} from the {@code X-elastic-product-origin} header:
 * one value per Elastic product in {@link MetricAttributes#KNOWN_PRODUCT_ORIGINS}, labelled with that origin, {@link #OTHER} for any
 * other origin, and {@link #REST} for a request without the header.
 */
public enum QueryMetric {
    KIBANA("kibana"),
    FLEET("fleet"),
    LOGSTASH("logstash"),
    OBSERVABILITY("observability"),
    CONNECTORS("connectors"),
    ELASTIC_SYNTHETICS("elastic-synthetics"),
    OTHER("other"),
    REST("rest");

    private final String label;

    QueryMetric(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return label;
    }
}
