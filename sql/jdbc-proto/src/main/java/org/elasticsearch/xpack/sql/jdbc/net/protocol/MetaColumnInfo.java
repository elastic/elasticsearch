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

public class MetaColumnInfo {
    public final String table, name;
    public final JDBCType type;
    public final int size, position;

    public MetaColumnInfo(String table, String name, JDBCType type, int size, int position) {
        if (table == null) {
            throw new IllegalArgumentException("[table] must not be null");
        }
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null");
        }
        this.table = table;
        this.name = name;
        this.type = type;
        this.size = size;
        this.position = position;
    }

    MetaColumnInfo(DataInput in) throws IOException {
        table = in.readUTF();
        name = in.readUTF();
        type = JDBCType.valueOf(in.readInt());
        size = in.readInt();
        position = in.readInt();
    }

    void write(DataOutput out) throws IOException {
        out.writeUTF(table);
        out.writeUTF(name);
        out.writeInt(type.getVendorTypeNumber());
        out.writeInt(size);
        out.writeInt(position);
    }

    @Override
    public String toString() {
        return table + "." + name
                + "<type=[" + type
                + "] size=[" + size
                + "] position=[" + position + "]>";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MetaColumnInfo other = (MetaColumnInfo) obj;
        return table.equals(other.table)
                && name.equals(other.name)
                && type.equals(other.type)
                && size == other.size
                && position == other.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, name, type, size, position);
    }
}
