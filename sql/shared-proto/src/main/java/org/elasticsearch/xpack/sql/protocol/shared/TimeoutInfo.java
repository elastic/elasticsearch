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
import java.util.concurrent.TimeUnit;

/**
 * Common class handling timeouts. Due to the nature of JDBC, all timeout values are expressed as millis.
 * Contains 
 */
public class TimeoutInfo {

    public static final long DEFAULT_REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(90);
    public static final long DEFAULT_PAGE_TIMEOUT = TimeUnit.SECONDS.toMillis(45);

    // client time - millis since epoch of when the client made the request
    // request timeout - how long the client is willing to wait for the server to process its request
    // page timeout - how long retrieving the next page (of the query) should take (this is used to scroll across pages)
    public final long clientTime, requestTimeout, pageTimeout;

    public TimeoutInfo(long clientTime, long timeout, long requestTimeout) {
        this.clientTime = clientTime;
        this.requestTimeout = timeout;
        this.pageTimeout = requestTimeout;
    }

    TimeoutInfo(DataInput in) throws IOException {
        clientTime = in.readLong();
        requestTimeout = in.readLong();
        pageTimeout = in.readLong();
    }

    void writeTo(DataOutput out) throws IOException {
        out.writeLong(clientTime);
        out.writeLong(requestTimeout);
        out.writeLong(pageTimeout);
    }

    @Override
    public String toString() {
        return "client=" + clientTime + ",request=" + requestTimeout + ",page=" + pageTimeout;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        TimeoutInfo other = (TimeoutInfo) obj;
        return clientTime == other.clientTime
                && requestTimeout == other.requestTimeout
                && pageTimeout == other.pageTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientTime, requestTimeout, pageTimeout);
    }
}
