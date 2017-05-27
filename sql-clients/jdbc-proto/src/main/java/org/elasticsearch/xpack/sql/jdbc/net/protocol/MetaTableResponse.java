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

public class MetaTableResponse extends Response {

    public static final MetaTableResponse EMPTY = new MetaTableResponse(emptyList());

    public final List<String> tables;

    public MetaTableResponse(List<String> tables) {
        super(Action.META_TABLE);
        this.tables = tables;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action));
        out.writeInt(tables.size());
        for (String t : tables) {
            out.writeUTF(t);
        }
    }

    public static MetaTableResponse decode(DataInput in) throws IOException {
        int length = in.readInt();
        if (length < 1) {
            return MetaTableResponse.EMPTY;
        }

        List<String> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            list.add(in.readUTF());
        }
        return new MetaTableResponse(list);
    }

    @Override
    public String toString() {
        return tables.toString();
    }
}
