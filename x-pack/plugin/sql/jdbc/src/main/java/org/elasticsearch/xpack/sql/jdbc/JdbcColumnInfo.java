/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import java.util.Objects;

import static org.elasticsearch.xpack.sql.client.StringUtils.EMPTY;

class JdbcColumnInfo {
    public final String catalog;
    public final String schema;
    public final String table;
    public final String label;
    public final String name;
    public final int displaySize;
    public final EsType type;

    JdbcColumnInfo(String name, EsType type, String table, String catalog, String schema, String label, int displaySize) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null");
        }
        if (table == null) {
            throw new IllegalArgumentException("[table] must not be null");
        }
        if (catalog == null) {
            throw new IllegalArgumentException("[catalog] must not be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("[schema] must not be null");
        }
        if (label == null) {
            throw new IllegalArgumentException("[label] must not be null");
        }
        this.name = name;
        this.type = type;
        this.table = table;
        this.catalog = catalog;
        this.schema = schema;
        this.label = label;
        this.displaySize = displaySize;
    }

    int displaySize() {
        // 0 - means unknown
        return displaySize;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (false == EMPTY.equals(table)) {
            b.append(table).append('.');
        }
        b.append(name).append("<type=[").append(type).append(']');
        if (false == EMPTY.equals(catalog)) {
            b.append(" catalog=[").append(catalog).append(']');
        }
        if (false == EMPTY.equals(schema)) {
            b.append(" schema=[").append(schema).append(']');
        }
        if (false == EMPTY.equals(label)) {
            b.append(" label=[").append(label).append(']');
        }
        return b.append('>').toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        JdbcColumnInfo other = (JdbcColumnInfo) obj;
        return name.equals(other.name)
                && type.equals(other.type)
                && table.equals(other.table)
                && catalog.equals(other.catalog)
                && schema.equals(other.schema)
                && label.equals(other.label)
                && displaySize == other.displaySize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, table, catalog, schema, label, displaySize);
    }
}
