/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles the configuration and parsing of settings for the <code>xpack.http.</code> prefix
 */
public class HttpSettings {

    private static final TimeValue DEFAULT_READ_TIMEOUT = TimeValue.timeValueSeconds(10);
    private static final TimeValue DEFAULT_CONNECTION_TIMEOUT = DEFAULT_READ_TIMEOUT;

    static final Setting<TimeValue> READ_TIMEOUT = Setting.timeSetting("xpack.http.default_read_timeout",
            DEFAULT_READ_TIMEOUT, Property.NodeScope);
    static final Setting<TimeValue> CONNECTION_TIMEOUT = Setting.timeSetting("xpack.http.default_connection_timeout",
            DEFAULT_CONNECTION_TIMEOUT, Property.NodeScope);


    // these are very apache http client specific settings, which only apply to how the apache http client is working
    // keep them in their own namespace, so that we could for example switch to the new java http client to get rid
    // of another dependency in the future
    // more information https://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html

    // should idle connections be evicted? This will start an additional thread doing this
    static final Setting<Boolean> APACHE_HTTP_CLIENT_EVICT_IDLE_CONNECTIONS =
        Setting.boolSetting("xpack.http.apache.evict_idle_connections", false, Property.NodeScope);
    // what is the timeout for evicting idle connections
    // this prevents form many connections being open due to the pooled client
    // this value resembles the default set in org.apache.http.impl.client.HttpClientBuilder.build()
    static final Setting<TimeValue> APACHE_HTTP_CLIENT_EVICT_IDLE_CONNECTIONS_TIMEOUT =
        Setting.timeSetting("xpack.http.apache.evict_idle_connections_timeout", TimeValue.timeValueSeconds(10), Property.NodeScope);
    // how many total connections should the http client be able to keep open at once
    static final Setting<Integer> APACHE_HTTP_CLIENT_MAX_CONN_TOTAL =
        Setting.intSetting("xpack.http.apache.max_conn_total", 100, 1, Property.NodeScope);
    // how many total connections per route should the http client be able to keep open at once
    // this for example defines how often a user is able to poll the same _search endpoint of a remote cluster, which is
    // also the reason why this is set to the same value than the total connections
    static final Setting<Integer> APACHE_HTTP_CLIENT_MAX_CONN_PER_ROUTE =
        Setting.intSetting("xpack.http.apache.max_conn_total_per_route", APACHE_HTTP_CLIENT_MAX_CONN_TOTAL, 1, Property.NodeScope);

    private static final String PROXY_HOST_KEY = "xpack.http.proxy.host";
    private static final String PROXY_PORT_KEY = "xpack.http.proxy.port";
    private static final String PROXY_SCHEME_KEY = "xpack.http.proxy.scheme";
    private static final String SSL_KEY_PREFIX = "xpack.http.ssl.";

    static final Setting<String> PROXY_HOST = Setting.simpleString(PROXY_HOST_KEY, Property.NodeScope);
    static final Setting<String> PROXY_SCHEME = Setting.simpleString(PROXY_SCHEME_KEY, (v, s) -> Scheme.parse(v), Property.NodeScope);
    static final Setting<Integer> PROXY_PORT = Setting.intSetting(PROXY_PORT_KEY, 0, 0, 0xFFFF, Property.NodeScope);

    static final Setting<ByteSizeValue> MAX_HTTP_RESPONSE_SIZE = Setting.byteSizeSetting("xpack.http.max_response_size",
            new ByteSizeValue(10, ByteSizeUnit.MB),   // default
            new ByteSizeValue(1, ByteSizeUnit.BYTES), // min
            new ByteSizeValue(50, ByteSizeUnit.MB),   // max
            Property.NodeScope);

    private static final SSLConfigurationSettings SSL = SSLConfigurationSettings.withPrefix(SSL_KEY_PREFIX);

    public static List<? extends Setting<?>> getSettings() {
        final ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(SSL.getAllSettings());
        settings.add(READ_TIMEOUT);
        settings.add(CONNECTION_TIMEOUT);
        settings.add(PROXY_HOST);
        settings.add(PROXY_PORT);
        settings.add(PROXY_SCHEME);
        settings.add(MAX_HTTP_RESPONSE_SIZE);
        settings.add(APACHE_HTTP_CLIENT_EVICT_IDLE_CONNECTIONS);
        settings.add(APACHE_HTTP_CLIENT_EVICT_IDLE_CONNECTIONS_TIMEOUT);
        settings.add(APACHE_HTTP_CLIENT_MAX_CONN_TOTAL);
        settings.add(APACHE_HTTP_CLIENT_MAX_CONN_PER_ROUTE);
        return settings;
    }

    private HttpSettings() {
    }
}
