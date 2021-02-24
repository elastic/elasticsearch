/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import java.util.concurrent.TimeUnit;

class HttpClientSettings {
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int DEFAULT_CONNECTION_POOL_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(50);

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int connectionPoolTimeoutMs = DEFAULT_CONNECTION_POOL_TIMEOUT_MILLIS;
    private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
    private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MILLIS;

    void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    void setConnectionPoolTimeoutMs(int connectionPoolTimeoutMs) {
        this.connectionPoolTimeoutMs = connectionPoolTimeoutMs;
    }

    void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    int getMaxRetries() {
        return maxRetries;
    }

    int getConnectionPoolTimeoutMs() {
        return connectionPoolTimeoutMs;
    }

    int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }
}
