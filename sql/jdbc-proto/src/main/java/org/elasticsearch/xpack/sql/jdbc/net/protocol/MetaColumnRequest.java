/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataOutput;

import java.io.IOException;
import java.util.Objects;

public class MetaColumnRequest extends Request {
    private final String tablePattern, columnPattern;

    public MetaColumnRequest(String tablePattern, String columnPattern) {
        this.tablePattern = tablePattern == null ? "" : tablePattern;
        this.columnPattern = columnPattern == null ? "" : columnPattern;
    }

    MetaColumnRequest(SqlDataInput in) throws IOException {
        tablePattern = in.readUTF();
        columnPattern = in.readUTF();
    }

    @Override
    protected void writeTo(SqlDataOutput out) throws IOException {
        out.writeUTF(tablePattern);
        out.writeUTF(columnPattern);
    }

    public String tablePattern() {
        return tablePattern;
    }

    public String columnPattern() {
        return columnPattern;
    }

    @Override
    protected String toStringBody() {
        return "table=[" + tablePattern
                + "] column=[" + columnPattern + "]";
    }

    @Override
    public RequestType requestType() {
        return RequestType.META_COLUMN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MetaColumnRequest other = (MetaColumnRequest) obj;
        return Objects.equals(tablePattern, other.tablePattern)
                && Objects.equals(columnPattern, other.columnPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePattern, columnPattern);
    }
}