/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetaTableRequest extends Request {
    private final String pattern;

    public MetaTableRequest(String pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("[pattern] must not be null");
        }
        this.pattern = pattern;
    }

    MetaTableRequest(int clientVersion, DataInput in) throws IOException {
        this.pattern = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pattern);
    }

    public String pattern() {
        return pattern;
    }

    @Override
    protected String toStringBody() {
        return pattern;
    }

    @Override
    public RequestType requestType() {
        return RequestType.META_TABLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MetaTableRequest other = (MetaTableRequest) obj;
        return pattern.equals(other.pattern);
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }
}
