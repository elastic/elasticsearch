/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import java.net.URI;

/** A dataset names a table without being able to override its parent's network endpoint. */
record ClickHouseTable(String database, String table) {
    static ClickHouseTable parse(String resource) {
        try {
            URI uri = URI.create(resource);
            String database = uri.getRawAuthority();
            String path = uri.getRawPath();
            if ("clickhouse".equals(uri.getScheme())
                && database != null
                && database.matches("[A-Za-z_][A-Za-z0-9_]*")
                && path != null
                && path.matches("/[A-Za-z_][A-Za-z0-9_]*")
                && uri.getQuery() == null
                && uri.getFragment() == null) {
                return new ClickHouseTable(database, path.substring(1));
            }
        } catch (IllegalArgumentException e) {
            // Report the supported grammar without reflecting user input.
        }
        throw new IllegalArgumentException("ClickHouse resource must be clickhouse://database/table with simple SQL identifiers");
    }

    String sql() {
        return quote(database) + "." + quote(table);
    }

    static String quote(String identifier) {
        return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }
}
