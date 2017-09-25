/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

public abstract class AbstractQueryPageRequest extends Request {
    public final byte[] cursor;
    public final TimeoutInfo timeout;

    protected AbstractQueryPageRequest(byte[] cursor, TimeoutInfo timeout) {
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
        this.cursor = new byte[ProtoUtil.readArraySize(in)];
        in.readFully(cursor);
        this.timeout = new TimeoutInfo(in);
    }

    @Override
    public void writeTo(SqlDataOutput out) throws IOException {
        out.writeInt(cursor.length);
        out.write(cursor);
        timeout.writeTo(out);
    }

    @Override
    protected String toStringBody() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < cursor.length; i++) {
            b.append(String.format(Locale.ROOT, "%02x", cursor[i]));
        }
        return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractQueryPageRequest other = (AbstractQueryPageRequest) obj;
        return Arrays.equals(cursor, other.cursor)
                && timeout.equals(other.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, timeout);
    }
}
