/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

public class RequestMeta {

    private int fetchSize;
    private long timeoutInMs;

    public RequestMeta() {
        this(0, 0);
    }

    public RequestMeta(int fetchSize, int timeout) {
        this.fetchSize = fetchSize;
        this.timeoutInMs = timeout;
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

}
