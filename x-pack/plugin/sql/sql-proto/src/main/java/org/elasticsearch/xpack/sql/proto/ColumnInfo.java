/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.proto;

import java.util.Objects;

/**
 * Information about a column returned with first query response.
 * As this represents the response for all drivers, it is important for it to be explicit about
 * its structure, in particular types (using es_type instead of DataType).
 */
public class ColumnInfo {

    private final String table;
    private final String name;
    private final String esType;
    private final Integer displaySize;

    public ColumnInfo(String table, String name, String esType, Integer displaySize) {
        this.table = table;
        this.name = name;
        this.esType = esType;
        this.displaySize = displaySize;
    }

    public ColumnInfo(String table, String name, String esType) {
        this.table = table;
        this.name = name;
        this.esType = esType;
        this.displaySize = null;
    }

    /**
     * Name of the table.
     */
    public String table() {
        return table;
    }

    /**
     * Name of the column.
     */
    public String name() {
        return name;
    }

    /**
     * The type of the column in Elasticsearch.
     */
    public String esType() {
        return esType;
    }

    /**
     * Used by JDBC
     */
    public Integer displaySize() {
        return displaySize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnInfo that = (ColumnInfo) o;
        return Objects.equals(displaySize, that.displaySize)
            && Objects.equals(table, that.table)
            && Objects.equals(name, that.name)
            && Objects.equals(esType, that.esType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, name, esType, displaySize);
    }

    @Override
    public String toString() {
        return "ColumnInfo{" + "table='" + table + "', name='" + name + "', esType='" + esType + "', displaySize=" + displaySize + '}';
    }
}
