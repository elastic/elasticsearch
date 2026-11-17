/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;

public class HttpSettings {
    // These settings are default scope for testing
    static final Setting<ByteSizeValue> MAX_HTTP_RESPONSE_SIZE = Setting.byteSizeSetting(
        "xpack.inference.http.max_response_size",
        ByteSizeValue.of(50, ByteSizeUnit.MB),   // default
        ByteSizeValue.ONE, // min
        ByteSizeValue.of(100, ByteSizeUnit.MB),   // max
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // The time we wait for a connection to establish
    public static final Setting<TimeValue> CONNECTION_TIMEOUT = Setting.timeSetting(
        "xpack.inference.http.connect_timeout",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    // Upper bound for how long a leased connection can idle
    public static final Setting<TimeValue> SOCKET_TIMEOUT = Setting.timeSetting(
        "xpack.inference.http.socket_timeout",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private volatile ByteSizeValue maxResponseSize;
    private final int connectionTimeout;
    private final ProxySettings proxySettings;

    public HttpSettings(Settings settings, ClusterService clusterService) {
        Objects.requireNonNull(clusterService);
        Objects.requireNonNull(settings);
        maxResponseSize = MAX_HTTP_RESPONSE_SIZE.get(settings);
        connectionTimeout = Math.toIntExact(CONNECTION_TIMEOUT.get(settings).getMillis());
        proxySettings = ProxySettings.fromSettings(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_HTTP_RESPONSE_SIZE, this::setMaxResponseSize);
    }

    public ByteSizeValue getMaxResponseSize() {
        return maxResponseSize;
    }

    public Duration connectionTimeoutDuration() {
        return Duration.ofMillis(connectionTimeout);
    }

    public ProxySettings proxy() {
        return proxySettings;
    }

    private void setMaxResponseSize(ByteSizeValue maxResponseSize) {
        this.maxResponseSize = maxResponseSize;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        final List<Setting<?>> settings = new ArrayList<>();

        settings.addAll(List.of(MAX_HTTP_RESPONSE_SIZE, CONNECTION_TIMEOUT, SOCKET_TIMEOUT));
        settings.addAll(ProxySettings.getSettingsDefinitions());

        return settings;
    }

    public static class ProxySettings {
        public static final String INFERENCE_PROXY_HOST_KEY = "xpack.inference.http.proxy.host";
        public static final String INFERENCE_PROXY_PORT_KEY = "xpack.inference.http.proxy.port";
        public static final String INFERENCE_PROXY_SCHEME_KEY = "xpack.inference.http.proxy.scheme";

        // These settings are default scope for testing
        static final Setting<String> INFERENCE_PROXY_HOST = Setting.simpleString(INFERENCE_PROXY_HOST_KEY, Setting.Property.NodeScope);

        static final Setting<String> INFERENCE_PROXY_SCHEME = Setting.simpleString(
            INFERENCE_PROXY_SCHEME_KEY,
            new ProxySettings.ProxySchemeValidator(),
            Setting.Property.NodeScope
        );

        static final Setting<Integer> INFERENCE_PROXY_PORT = Setting.intSetting(
            INFERENCE_PROXY_PORT_KEY,
            0,
            0,
            0xFFFF,
            Setting.Property.NodeScope
        );

        private final String host;
        private final Scheme scheme;
        private final Integer port;

        public static ProxySettings fromSettings(Settings settings) {
            if (settings.hasValue(INFERENCE_PROXY_HOST_KEY) == false) {
                return null;
            }
            return new ProxySettings(settings);
        }

        public ProxySettings(Settings settings) {
            var schemeValue = settings.get(INFERENCE_PROXY_SCHEME_KEY);
            this.scheme = schemeValue == null ? Scheme.HTTP : Scheme.parse(schemeValue);
            this.host = settings.get(INFERENCE_PROXY_HOST_KEY);
            this.port = settings.getAsInt(INFERENCE_PROXY_PORT_KEY, scheme.defaultPort());
        }

        public String host() {
            return host;
        }

        public Scheme scheme() {
            return scheme;
        }

        public Integer port() {
            return port;
        }

        public static List<Setting<?>> getSettingsDefinitions() {
            return List.of(INFERENCE_PROXY_HOST, INFERENCE_PROXY_PORT, INFERENCE_PROXY_SCHEME);
        }

        /**
         * The schemes supported for connecting to the proxy. Only {@code http} is supported: the proxy is asked to tunnel the HTTPS
         * connection to the inference provider via {@code CONNECT}. An {@code https} proxy would require layering the provider's TLS
         * session on top of the TLS connection to the proxy, which the Apache async client cannot do (a connection only supports a
         * single TLS session, see {@code InternalDataChannel#startTls} in httpcore5), so it is rejected instead of failing every request.
         */
        public enum Scheme {
            HTTP("http", 80);

            private final String scheme;
            private final int defaultPort;

            Scheme(String scheme, int defaultPort) {
                this.scheme = scheme;
                this.defaultPort = defaultPort;
            }

            public String scheme() {
                return this.scheme;
            }

            public int defaultPort() {
                return defaultPort;
            }

            public static Scheme parse(String value) {
                return switch (value) {
                    case "http" -> HTTP;
                    default -> throw new IllegalArgumentException(unsupportedSchemeMessage(value));
                };
            }
        }

        static String unsupportedSchemeMessage(String value) {
            return format(
                "%s only supports [http] but was [%s]. The proxy is used to tunnel HTTPS connections to inference providers using CONNECT, "
                    + "[https] proxies are not supported",
                INFERENCE_PROXY_SCHEME_KEY,
                value
            );
        }

        // visible for testing
        static class ProxySchemeValidator implements Setting.Validator<String> {

            @Override
            public void validate(String value) {
                Scheme.parse(Objects.requireNonNull(value, () -> format("%s cannot be [null]", INFERENCE_PROXY_SCHEME_KEY)));
            }
        }
    }
}
