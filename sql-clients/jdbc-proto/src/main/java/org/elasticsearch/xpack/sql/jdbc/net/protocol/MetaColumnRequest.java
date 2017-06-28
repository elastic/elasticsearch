/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.StringUtils.nullAsEmpty;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.StringUtils.splitToIndexAndType;

public class MetaColumnRequest extends Request {

    private final String tablePattern, columnPattern;
    public final String index, type, column;

    public MetaColumnRequest(String tablePattern, String columnPattern) {
        super(Action.META_COLUMN);

        this.tablePattern = nullAsEmpty(tablePattern);
        this.columnPattern = nullAsEmpty(columnPattern);

        String[] split = splitToIndexAndType(tablePattern);

        this.index = split[0];
        this.type = split[1];
        this.column = nullAsEmpty(columnPattern);
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
        out.writeUTF(tablePattern);
        out.writeUTF(columnPattern);
    }

    public static MetaColumnRequest decode(DataInput in) throws IOException {
        String tablePattern = in.readUTF();
        String columnPattern = in.readUTF();
        return new MetaColumnRequest(tablePattern, columnPattern);
    }

    @Override
    public String toString() {
        return "MetaColumn[index=" + index + ", type=" + type + " column=" + column + "]";
    }
}