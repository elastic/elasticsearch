/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class MetaTableResponse extends Response {
    public final List<String> tables;

    public MetaTableResponse(List<String> tables) {
        if (tables == null) {
            throw new IllegalArgumentException("[tables] must not be null");
        }
        this.tables = tables;
    }

    MetaTableResponse(Request request, DataInput in) throws IOException {
        int length = in.readInt();
        List<String> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(in.readUTF());
        }
        tables = unmodifiableList(list);
    }

    @Override
    public void write(int clientVersion, DataOutput out) throws IOException {
        out.writeInt(tables.size());
        for (String t : tables) {
            out.writeUTF(t);
        }
    }

    @Override
    protected String toStringBody() {
        return String.join(", ", tables);
    }

    @Override
    public RequestType requestType() {
        return RequestType.META_TABLE;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.META_TABLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MetaTableResponse other = (MetaTableResponse) obj;
        return tables.equals(other.tables);
    }

    @Override
    public int hashCode() {
        return tables.hashCode();
    }

}
