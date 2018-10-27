/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;

public class HipChatServer {

    public static final String HOST_SETTING = "host";
    public static final String PORT_SETTING = "port";

    public static final HipChatServer DEFAULT = new HipChatServer("api.hipchat.com", 443, null);

    private final String host;
    private final int port;
    private final HipChatServer fallback;

    public HipChatServer(Settings settings) {
        this(settings, DEFAULT);
    }

    public HipChatServer(Settings settings, HipChatServer fallback) {
        this(settings.get(HOST_SETTING, null), settings.getAsInt(PORT_SETTING, -1), fallback);
    }

    public HipChatServer(String host, int port, HipChatServer fallback) {
        this.host = host;
        this.port = port;
        this.fallback = fallback;
    }

    public String host() {
        return host != null ? host : fallback.host();
    }

    public int port() {
        return port > 0 ? port : fallback.port();
    }

    public HipChatServer fallback() {
        return fallback != null ? fallback : DEFAULT;
    }

    public HipChatServer rebuild(Settings settings, HipChatServer fallback) {
        return new HipChatServer(settings.get(HOST_SETTING, host), settings.getAsInt(PORT_SETTING, port), fallback);
    }

    public synchronized HttpRequest.Builder httpRequest() {
        return HttpRequest.builder(host(), port());
    }

}
