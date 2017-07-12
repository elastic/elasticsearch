/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.Nullable;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class QueryPageRequest extends Request {
    public final String requestId;
    public final TimeoutInfo timeout;
    private final transient Page data;

    public QueryPageRequest(String requestId, TimeoutInfo timeout, @Nullable Page data) {
        if (requestId == null) {
            throw new IllegalArgumentException("[requestId] must not be null");
        }
        if (timeout == null) {
            throw new IllegalArgumentException("[timeout] must not be null");
        }
        this.requestId = requestId;
        this.timeout = timeout;
        this.data = data;
    }

    QueryPageRequest(int clientVersion, DataInput in) throws IOException {
        this.requestId = in.readUTF();
        this.timeout = new TimeoutInfo(in);
        this.data = null; // Data isn't used on the server side
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(requestId);
        timeout.write(out);
    }

    public Page data() {
        return data;
    }

    @Override
    protected String toStringBody() {
        return requestId;
    }

    @Override
    public RequestType requestType() {
        return RequestType.QUERY_PAGE;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        QueryPageRequest other = (QueryPageRequest) obj;
        return requestId.equals(other.requestId)
                && timeout.equals(other.timeout);
        // data is intentionally ignored
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, timeout);
        // data is intentionally ignored
    }
}
