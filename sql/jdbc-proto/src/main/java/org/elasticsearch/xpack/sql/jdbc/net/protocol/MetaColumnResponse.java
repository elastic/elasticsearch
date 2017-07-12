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
import static java.util.stream.Collectors.joining;

public class MetaColumnResponse extends Response {
    public final List<MetaColumnInfo> columns;

    public MetaColumnResponse(List<MetaColumnInfo> columns) {
        if (columns == null) {
            throw new IllegalArgumentException("[columns] must not be null");
        }
        this.columns = columns;
    }

    public MetaColumnResponse(Request request, DataInput in) throws IOException {
        int length = in.readInt();
        List<MetaColumnInfo> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            list.add(new MetaColumnInfo(in));
        }
        columns = unmodifiableList(list);
    }

    @Override
    protected void write(int clientVersion, DataOutput out) throws IOException {
        out.writeInt(columns.size());
        for (MetaColumnInfo info : columns) {
            info.write(out);
        }
    }

    @Override
    protected String toStringBody() {
        return columns.stream().map(Object::toString).collect(joining(", "));
    }

    @Override
    public RequestType requestType() {
        return RequestType.META_COLUMN;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.META_COLUMN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MetaColumnResponse other = (MetaColumnResponse) obj;
        return columns.equals(other.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }
}