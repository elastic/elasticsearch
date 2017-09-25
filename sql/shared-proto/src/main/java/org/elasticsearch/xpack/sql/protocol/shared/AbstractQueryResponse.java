/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/**
 * Superclass for responses both for {@link AbstractQueryInitRequest}
 * and {@link AbstractQueryPageRequest}.
 */
public abstract class AbstractQueryResponse extends Response {
    private final long tookNanos;
    private final byte[] cursor;

    protected AbstractQueryResponse(long tookNanos, byte[] cursor) {
        if (cursor == null) {
            throw new IllegalArgumentException("cursor must not be null");
        }
        this.tookNanos = tookNanos;
        this.cursor = cursor;
    }

    protected AbstractQueryResponse(Request request, DataInput in) throws IOException {
        tookNanos = in.readLong();
        cursor = new byte[ProtoUtil.readArraySize(in)];
        in.readFully(cursor);
    }

    @Override
    protected void writeTo(SqlDataOutput out) throws IOException {
        out.writeLong(tookNanos);
        out.writeInt(cursor.length);
        out.write(cursor);
    }

    /**
     * How long the request took on the server as measured by
     * {@link System#nanoTime()}.
     */
    public long tookNanos() {
        return tookNanos;
    }

    /**
     * Cursor for fetching the next page. If it has {@code length = 0}
     * then there is no next page.
     */
    public byte[] cursor() {
        return cursor;
    }

    @Override
    protected String toStringBody() {
        StringBuilder b = new StringBuilder();
        b.append("tookNanos=[").append(tookNanos);
        b.append("] cursor=[");
        for (int i = 0; i < cursor.length; i++) {
            b.append(String.format(Locale.ROOT, "%02x", cursor[i]));
        }
        b.append("]");
        return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractQueryResponse other = (AbstractQueryResponse) obj;
        return tookNanos == other.tookNanos
                && Arrays.equals(cursor, other.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tookNanos, Arrays.hashCode(cursor));
    }
}
