/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.TimeZone;

public abstract class AbstractQueryInitRequest extends Request {
    /**
     * Global choice for the default fetch size.
     */
    public static final int DEFAULT_FETCH_SIZE = 1000;

    public final String query;
    public final int fetchSize;
    public final TimeZone timeZone;
    public final TimeoutInfo timeout;

    protected AbstractQueryInitRequest(String query, int fetchSize, TimeZone timeZone, TimeoutInfo timeout) {
        this.query = query;
        this.fetchSize = fetchSize;
        this.timeZone = timeZone;
        this.timeout = timeout;
    }

    protected AbstractQueryInitRequest(int clientVersion, DataInput in) throws IOException {
        query = in.readUTF();
        fetchSize = in.readInt();
        timeZone = TimeZone.getTimeZone(in.readUTF());
        timeout = new TimeoutInfo(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(query);
        out.writeInt(fetchSize);
        out.writeUTF(timeZone.getID());
        timeout.write(out);
    }

    @Override
    protected String toStringBody() {
        StringBuilder b = new StringBuilder();
        b.append("query=[").append(query).append(']');
        if (false == timeZone.getID().equals("UTC")) {
            b.append(" timeZone=[").append(timeZone.getID()).append(']');
        }
        return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractQueryInitRequest other = (AbstractQueryInitRequest) obj;
        return fetchSize == other.fetchSize
                && Objects.equals(query, other.query)
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(timeZone.getID(), other.timeZone.getID());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchSize, query, timeout, timeZone.getID().hashCode());
    }

}
