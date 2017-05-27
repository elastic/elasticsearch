/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

public class ColumnInfo {

    public String catalog, schema, table, label, name;
    public int type;

    public ColumnInfo() {}

    public ColumnInfo(String columnName, int columnType, 
            String tableName,
            String catalogName, 
            String schemaName, 
            String columnLabel) {
        this.type = columnType;
        this.catalog = catalogName;
        this.table = tableName;
        this.label = columnLabel;
        this.name = columnName;
        this.schema = schemaName;
    }

    public int displaySize() {
        return -1;
    }

    @Override
    public String toString() {
        return name;
    }
}