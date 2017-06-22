/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Status;

import static java.util.Collections.emptyList;

public class MetaColumnResponse extends Response {

    public static final MetaColumnResponse EMPTY = new MetaColumnResponse(emptyList());

    public final List<MetaColumnInfo> columns;

    public MetaColumnResponse(List<MetaColumnInfo> columns) {
        super(Action.META_COLUMN);
        this.columns = columns;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action));
        out.writeInt(columns.size());

        for (MetaColumnInfo info : columns) {
            // NOCOMMIT core would make MetaColumnInfo know how to read and write itself which feels cleaner. 
            out.writeUTF(info.name);
            out.writeUTF(info.table);
            out.writeInt(info.type);
            out.writeInt(info.size);
            out.writeInt(info.position);
        }
    }

    public static MetaColumnResponse decode(DataInput in) throws IOException {
        int length = in.readInt();

        if (length < 1) {
            return MetaColumnResponse.EMPTY;
        }
        List<MetaColumnInfo> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            String name = in.readUTF();
            String table = in.readUTF();
            int type = in.readInt();
            int size = in.readInt();
            int pos = in.readInt();
            list.add(new MetaColumnInfo(name, table, type, size, pos));
        }

        return new MetaColumnResponse(list);
    }

    @Override
    public String toString() {
        return columns.toString();
    }
}