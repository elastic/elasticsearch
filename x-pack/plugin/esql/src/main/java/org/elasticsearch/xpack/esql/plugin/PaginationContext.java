/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import java.time.ZoneId;
import java.util.Map;

/**
 * Holds pagination parameters that flow from the transport action through the session
 * and compute service into the {@link StorePageOperator}.
 */
public record PaginationContext(
    String cursorId,
    int pageSize,
    long expirationMillis,
    ZoneId zoneId,
    boolean columnar,
    Map<String, String> securityHeaders
) {
    public PaginationContext {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive, got [" + pageSize + "]");
        }
    }
}
