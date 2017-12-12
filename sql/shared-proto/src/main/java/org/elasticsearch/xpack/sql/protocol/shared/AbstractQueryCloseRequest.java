/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractQueryCloseRequest extends Request {
    public final String cursor;

    protected AbstractQueryCloseRequest(String cursor) {
        if (cursor == null) {
            throw new IllegalArgumentException("[cursor] must not be null");
        }
        this.cursor = cursor;
    }

    protected AbstractQueryCloseRequest(SqlDataInput in) throws IOException {
        this.cursor = in.readUTF();
    }

    @Override
    public void writeTo(SqlDataOutput out) throws IOException {
        out.writeUTF(cursor);
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
        AbstractQueryCloseRequest other = (AbstractQueryCloseRequest) obj;
        return Objects.equals(cursor, other.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor);
    }
}

