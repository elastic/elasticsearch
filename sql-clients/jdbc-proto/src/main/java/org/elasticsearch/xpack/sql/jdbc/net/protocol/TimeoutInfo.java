/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeoutInfo {

    public final long clientTime, timeout, requestTimeout;

    public TimeoutInfo(long clientTime, long timeout, long requestTimeout) {
        this.clientTime = clientTime;
        this.timeout = timeout;
        this.requestTimeout = requestTimeout;
    }

    void encode(DataOutput out) throws IOException {
        out.writeLong(clientTime);
        out.writeLong(timeout);
        out.writeLong(requestTimeout);
    }

    static TimeoutInfo readTimeout(DataInput in) throws IOException {
        long clientTime = in.readLong();
        long timeout = in.readLong();
        long requestTimeout = in.readLong();

        return new TimeoutInfo(clientTime, timeout, requestTimeout);
    }
}
