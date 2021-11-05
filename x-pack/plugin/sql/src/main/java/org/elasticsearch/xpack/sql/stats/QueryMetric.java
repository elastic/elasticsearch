/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.stats;

import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.util.Locale;

import static org.elasticsearch.xpack.sql.proto.RequestInfo.ODBC_CLIENT_IDS;

public enum QueryMetric {
    CANVAS,
    CLI,
    JDBC,
    ODBC,
    ODBC32,
    ODBC64,
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

    public static QueryMetric from(Mode mode, String clientId) {
        if (mode == Mode.ODBC) {
            // default to "odbc_32" if the client_id is not provided or it has a wrong value
            if (clientId == null || false == ODBC_CLIENT_IDS.contains(clientId)) {
                return fromString(RequestInfo.ODBC_32);
            } else {
                return fromString(clientId);
            }
        }
        return fromString(mode == Mode.PLAIN ? clientId : mode.toString());
    }
}
