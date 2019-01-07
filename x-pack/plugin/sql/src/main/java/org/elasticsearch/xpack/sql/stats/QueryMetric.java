/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.stats;

import org.elasticsearch.xpack.sql.proto.Mode;

import java.util.Locale;

public enum QueryMetric {
    CANVAS, CLI, JDBC, ODBC, REST;
    
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

    public static QueryMetric from(Mode mode, String clientId) {
        return fromString(mode == Mode.PLAIN ? clientId : mode.toString());
    }
}
