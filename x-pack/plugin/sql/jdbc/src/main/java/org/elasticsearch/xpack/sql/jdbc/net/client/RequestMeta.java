/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

public class RequestMeta {

    private int fetchSize;
    private long timeoutInMs;
    private long queryTimeoutInMs;

    public RequestMeta(int fetchSize, long timeout, long queryTimeoutInMs) {
        this.fetchSize = fetchSize;
        this.timeoutInMs = timeout;
        this.queryTimeoutInMs = queryTimeoutInMs;
    }

    public RequestMeta queryTimeout(long timeout) {
        this.queryTimeoutInMs = timeout;
        return this;
    }

    public RequestMeta timeout(long timeout) {
        this.timeoutInMs = timeout;
        return this;
    }

    public RequestMeta fetchSize(int size) {
        this.fetchSize = size;
        return this;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public long timeoutInMs() {
        return timeoutInMs;
    }

    public long queryTimeoutInMs() {
        return queryTimeoutInMs;
    }
}
