/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

class RequestMeta {

    private int fetchSize;
    private long timeoutInMs;
    private long queryTimeoutInMs;

    RequestMeta(int fetchSize, long timeout, long queryTimeoutInMs) {
        this.fetchSize = fetchSize;
        this.timeoutInMs = timeout;
        this.queryTimeoutInMs = queryTimeoutInMs;
    }

    RequestMeta queryTimeout(long timeout) {
        this.queryTimeoutInMs = timeout;
        return this;
    }

    RequestMeta timeout(long timeout) {
        this.timeoutInMs = timeout;
        return this;
    }

    RequestMeta fetchSize(int size) {
        this.fetchSize = size;
        return this;
    }

    int fetchSize() {
        return fetchSize;
    }

    long timeoutInMs() {
        return timeoutInMs;
    }

    long queryTimeoutInMs() {
        return queryTimeoutInMs;
    }
}
