/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConnectionConfiguration {

    public static class HostAndPort {
        public final String ip;
        public final int port;

        public HostAndPort(String ip) {
            this(ip, 0);
        }

        public HostAndPort(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        @Override
        public String toString() {
            return (port > 0 ? ip + ":" + port : ip);
        }
    }


    // Timeouts

    // 30s
    private static final String CONNECT_TIMEOUT = "connect.timeout";
    private static final String CONNECT_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.SECONDS.toMillis(30));

    // 1m
    private static final String NETWORK_TIMEOUT = "network.timeout";
    private static final String NETWORK_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.MINUTES.toMillis(1));

    // 1m
    private static final String QUERY_TIMEOUT = "query.timeout";
    private static final String QUERY_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.MINUTES.toMillis(1));

    // 5m
    private static final String PAGE_TIMEOUT = "page.timeout";
    private static final String PAGE_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.MINUTES.toMillis(5));

    private static final String PAGE_SIZE = "page.size";
    private static final String PAGE_SIZE_DEFAULT = "1000";

    // Auth

    private static final String AUTH_USER = "user";
    private static final String AUTH_PASS = "pass";

    // Proxy

    private final Properties settings;

    private long connectTimeout;
    private long networkTimeout;
    private long queryTimeout;

    private long pageTimeout;
    private int pageSize;

    private final String user, pass;


    private final SslConfig sslConfig;
    private final ProxyConfig proxyConfig;

    public ConnectionConfiguration(Properties props) {
        settings = props != null ? new Properties(props) : new Properties();

        connectTimeout = Long.parseLong(settings.getProperty(CONNECT_TIMEOUT, CONNECT_TIMEOUT_DEFAULT));
        networkTimeout = Long.parseLong(settings.getProperty(NETWORK_TIMEOUT, NETWORK_TIMEOUT_DEFAULT));
        queryTimeout = Long.parseLong(settings.getProperty(QUERY_TIMEOUT, QUERY_TIMEOUT_DEFAULT));
        // page
        pageTimeout = Long.parseLong(settings.getProperty(PAGE_TIMEOUT, PAGE_TIMEOUT_DEFAULT));
        pageSize = Integer.parseInt(settings.getProperty(PAGE_SIZE, PAGE_SIZE_DEFAULT));

        // auth
        user = settings.getProperty(AUTH_USER);
        pass = settings.getProperty(AUTH_PASS);

        sslConfig = new SslConfig(settings);
        proxyConfig = new ProxyConfig(settings);
    }

    protected boolean isSSLEnabled() {
        return sslConfig.isEnabled();
    }

    SslConfig sslConfig() {
        return sslConfig;
    }

    ProxyConfig proxyConfig() {
        return proxyConfig;
    }

    protected Properties settings() {
        return settings;
    }

    public void connectTimeout(long millis) {
        connectTimeout = millis;
    }

    public long connectTimeout() {
        return connectTimeout;
    }

    public void networkTimeout(long millis) {
        networkTimeout = millis;
    }

    public long networkTimeout() {
        return networkTimeout;
    }

    public void queryTimeout(long millis) {
        queryTimeout = millis;
    }

    public long queryTimeout() {
        return queryTimeout;
    }

    public long pageTimeout() {
        return pageTimeout;
    }

    public int pageSize() {
        return pageSize;
    }
    
    // auth

    public String authUser() {
        return user;
    }

    public String authPass() {
        return pass;
    }
}