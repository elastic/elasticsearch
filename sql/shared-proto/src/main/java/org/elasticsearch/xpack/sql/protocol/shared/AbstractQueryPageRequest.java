/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractQueryPageRequest extends Request {
    public final String cursor;
    public final TimeoutInfo timeout;

    protected AbstractQueryPageRequest(String cursor, TimeoutInfo timeout) {
        if (cursor == null) {
            throw new IllegalArgumentException("[cursor] must not be null");
        }
        if (timeout == null) {
            throw new IllegalArgumentException("[timeout] must not be null");
        }
        this.cursor = cursor;
        this.timeout = timeout;
    }

    protected AbstractQueryPageRequest(SqlDataInput in) throws IOException {
        this.cursor = in.readUTF();
        this.timeout = new TimeoutInfo(in);
    }

    @Override
    public void writeTo(SqlDataOutput out) throws IOException {
        out.writeUTF(cursor);
        timeout.writeTo(out);
    }

    @Override
    protected String toStringBody() {
        return cursor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractQueryPageRequest other = (AbstractQueryPageRequest) obj;
        return Objects.equals(cursor, other.cursor)
                && timeout.equals(other.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, timeout);
    }
}
