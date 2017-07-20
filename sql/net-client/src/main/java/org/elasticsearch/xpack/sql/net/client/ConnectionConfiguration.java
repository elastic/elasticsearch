/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client;

import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

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
    static final String CONNECT_TIMEOUT = "connect.timeout";
    static final String CONNECT_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.SECONDS.toMillis(30));

    // 1m
    static final String NETWORK_TIMEOUT = "network.timeout";
    static final String NETWORK_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.MINUTES.toMillis(1));

    // 1m
    static final String QUERY_TIMEOUT = "query.timeout";
    static final String QUERY_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.MINUTES.toMillis(1));

    // 5m
    static final String PAGE_TIMEOUT = "page.timeout";
    static final String PAGE_TIMEOUT_DEFAULT = String.valueOf(TimeUnit.MINUTES.toMillis(5));

    static final String PAGE_SIZE = "page.size";
    static final String PAGE_SIZE_DEFAULT = "1000";

    static final String SSL = "ssl";
    static final String SSL_DEFAULT = "false";

    static final String SSL_PROTOCOL = "ssl.protocol";
    static final String SSL_PROTOCOL_DEFAULT = "TLS"; // SSL alternative

    static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
    static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";

    static final String SSL_KEYSTORE_PASS = "ssl.keystore.location";
    static final String SSL_KEYSTORE_PASS_DEFAULT = "";

    static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
    static final String SSL_KEYSTORE_TYPE_DEFAULT = "JKS"; // PCKS12

    static final String SSL_TRUSTSTORE_LOCATION = "ssl.keystore.location";
    static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "";

    static final String SSL_TRUSTSTORE_PASS = "ssl.keystore.location";
    static final String SSL_TRUSTSTORE_PASS_DEFAULT = "";

    static final String SSL_TRUSTSTORE_TYPE = "ssl.keystore.location";
    static final String SSL_TRUSTSTORE_TYPE_DEFAULT = "ssl.keystore.location";
    

    private final Properties settings;

    private long connectTimeout;
    private long networkTimeout;
    private long queryTimeout;

    private long pageTimeout;
    private int pageSize;
    private final boolean ssl;

    public ConnectionConfiguration(Properties props) {
        settings = props != null ? new Properties(props) : new Properties();

        connectTimeout = Long.parseLong(settings.getProperty(CONNECT_TIMEOUT, CONNECT_TIMEOUT_DEFAULT));
        networkTimeout = Long.parseLong(settings.getProperty(NETWORK_TIMEOUT, NETWORK_TIMEOUT_DEFAULT));
        queryTimeout = Long.parseLong(settings.getProperty(QUERY_TIMEOUT, QUERY_TIMEOUT_DEFAULT));
        // page
        pageTimeout = Long.parseLong(settings.getProperty(PAGE_TIMEOUT, PAGE_TIMEOUT_DEFAULT));
        pageSize = Integer.parseInt(settings.getProperty(PAGE_SIZE, PAGE_SIZE_DEFAULT));
        ssl = StringUtils.parseBoolean(settings.getProperty(SSL, SSL_DEFAULT));
    }

    protected Properties settings() {
        return settings;
    }

    protected boolean isSSL() {
        return ssl;
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
}