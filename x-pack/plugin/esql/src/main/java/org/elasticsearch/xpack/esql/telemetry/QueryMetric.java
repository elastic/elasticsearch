/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import java.util.Locale;

/**
 * Who sent an ES|QL query, a closed set derived by {@link QueryCallerClassifier} from the {@code X-elastic-product-origin} header:
 * {@link #KIBANA} for Kibana, {@link #OTHER} for any other origin (the header is meant for Elastic products), and {@link #REST} for
 * a request without the header.
 */
public enum QueryMetric {
    KIBANA,
    REST,
    OTHER;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
