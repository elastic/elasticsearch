/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import java.util.Locale;

public enum QueryMetric {
    KIBANA,
    REST;

    public static QueryMetric fromString(String metric) {
        try {
            return QueryMetric.valueOf(metric.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            return REST;
        }
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
