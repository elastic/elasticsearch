/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.sql.JDBCType;
import java.util.Locale;

import static java.lang.String.format;

public class MetaColumnInfo {

    // column.name      - string - column name
    // table.name       - string - index.type
    // data.type        - int    - data type
    // column.size      - int    
    // ordinal.position - int    - position inside table
    public final String name, table;
    public final int type, size, position;

    public MetaColumnInfo(String name, String table, int type, int size, int position) {
        this.name = name;
        this.table = table;
        this.type = type;
        this.size = size;
        this.position = position;
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s,%s,%s,%d,%d", name, table, JDBCType.valueOf(type), size, position);
    }
}
