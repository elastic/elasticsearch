/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

class RequestMeta {

    private int fetchSize;
    private long pageTimeoutInMs;
    private long queryTimeoutInMs;

    RequestMeta(int fetchSize, long pageTimeoutInMs, long queryTimeoutInMs) {
        this.fetchSize = fetchSize;
        this.pageTimeoutInMs = pageTimeoutInMs;
        this.queryTimeoutInMs = queryTimeoutInMs;
    }

    RequestMeta queryTimeout(long timeout) {
        this.queryTimeoutInMs = timeout;
        return this;
    }

    RequestMeta pageTimeout(long timeout) {
        this.pageTimeoutInMs = timeout;
        return this;
    }

    RequestMeta fetchSize(int size) {
        this.fetchSize = size;
        return this;
    }

    int fetchSize() {
        return fetchSize;
    }

    long pageTimeoutInMs() {
        return pageTimeoutInMs;
    }

    long queryTimeoutInMs() {
        return queryTimeoutInMs;
    }
}
