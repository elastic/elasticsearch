/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.JDBCType;
import java.util.Objects;

public class ColumnInfo {
    public String catalog, schema, table, label, name;
    public JDBCType type;

    public ColumnInfo(String name, JDBCType type, String table, String catalog, String schema, String label) {
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
    }

    ColumnInfo(DataInput in) throws IOException {
        name = in.readUTF();
        type = JDBCType.valueOf(in.readInt());
        table = in.readUTF();
        catalog = in.readUTF();
        schema = in.readUTF();
        label = in.readUTF();
    }

    void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(type.getVendorTypeNumber());
        out.writeUTF(table);
        out.writeUTF(catalog);
        out.writeUTF(schema);
        out.writeUTF(label);
    }

    public int displaySize() {
        // NOCOMMIT look at this one.....
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (false == "".equals(table)) {
            b.append(table).append('.');
        }
        b.append(name).append("<type=[").append(type).append(']');
        if (false == "".equals(catalog)) {
            b.append(" catalog=[").append(catalog).append(']');
        }
        if (false == "".equals(schema)) {
            b.append(" schema=[").append(schema).append(']');
        }
        if (false == "".equals(label)) {
            b.append(" label=[").append(label).append(']');
        }
        return b.append('>').toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ColumnInfo other = (ColumnInfo) obj;
        return name.equals(other.name)
                && type.equals(other.type)
                && table.equals(other.table)
                && catalog.equals(other.catalog)
                && schema.equals(other.schema)
                && label.equals(other.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, table, catalog, schema, label);
    }
}
