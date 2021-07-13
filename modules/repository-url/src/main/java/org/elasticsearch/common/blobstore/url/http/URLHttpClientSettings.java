/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.TimeUnit;

public class URLHttpClientSettings {
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int DEFAULT_CONNECTION_POOL_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(50);

    static final Setting<TimeValue> SOCKET_TIMEOUT_SETTING = Setting.timeSetting(
        "http_socket_timeout",
        TimeValue.timeValueMillis(URLHttpClientSettings.DEFAULT_SOCKET_TIMEOUT_MILLIS),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(60));

    static final Setting<Integer> HTTP_MAX_RETRIES_SETTING = Setting.intSetting(
        "http_max_retries",
        URLHttpClientSettings.DEFAULT_MAX_RETRIES,
        0,
        Integer.MAX_VALUE);

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int connectionPoolTimeoutMs = DEFAULT_CONNECTION_POOL_TIMEOUT_MILLIS;
    private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
    private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MILLIS;

    public static URLHttpClientSettings fromSettings(Settings settings) {
        final URLHttpClientSettings httpClientSettings = new URLHttpClientSettings();
        httpClientSettings.setSocketTimeoutMs((int) SOCKET_TIMEOUT_SETTING.get(settings).millis());
        httpClientSettings.setMaxRetries(HTTP_MAX_RETRIES_SETTING.get(settings));
        return httpClientSettings;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setConnectionPoolTimeoutMs(int connectionPoolTimeoutMs) {
        this.connectionPoolTimeoutMs = connectionPoolTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getConnectionPoolTimeoutMs() {
        return connectionPoolTimeoutMs;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }
}
