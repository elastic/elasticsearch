/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa;

import java.util.Locale;

public enum QueryMetric {
    JDBC, ODBC, CLI, CANVAS, REST;
    
    public static QueryMetric fromString(String metric) {
        if (metric == null || metric.equalsIgnoreCase("plain")) {
            return REST;
        }
        return QueryMetric.valueOf(metric.toUpperCase(Locale.ROOT));
    }
    
    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
