/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.stats;

import java.util.Locale;

public enum Metric {
    JDBC, ODBC, CLI, CANVAS, REST;
    
    public static Metric fromString(String metric) {
        if (metric == null || metric.equalsIgnoreCase("plain")) {
            return REST;
        }
        return Metric.valueOf(metric.toUpperCase(Locale.ROOT));
    }
    
    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
