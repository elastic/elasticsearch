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
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

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

    private static final String PROXY_HOST_KEY = "xpack.http.proxy.host";
    private static final String PROXY_PORT_KEY = "xpack.http.proxy.port";
    private static final String PROXY_SCHEME_KEY = "xpack.http.proxy.scheme";
    private static final String SSL_KEY_PREFIX = "xpack.http.ssl.";

    static final Setting<String> PROXY_HOST = Setting.simpleString(PROXY_HOST_KEY, Property.NodeScope);
    static final Setting<String> PROXY_SCHEME = Setting.simpleString(PROXY_SCHEME_KEY, Scheme::parse, Property.NodeScope);
    static final Setting<Integer> PROXY_PORT = Setting.intSetting(PROXY_PORT_KEY, 0, 0, 0xFFFF, Property.NodeScope);
    static final Setting<List<String>> HOSTS_WHITELIST = Setting.listSetting("xpack.http.whitelist", Collections.singletonList("*"),
        Function.identity(), Property.NodeScope, Property.Dynamic);

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
        settings.add(HOSTS_WHITELIST);
        return settings;
    }

    private HttpSettings() {
    }
}
