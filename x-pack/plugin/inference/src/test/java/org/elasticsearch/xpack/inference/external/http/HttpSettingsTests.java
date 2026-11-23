/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.external.http.HttpSettings.ProxySettings.INFERENCE_PROXY_HOST_KEY;
import static org.elasticsearch.xpack.inference.external.http.HttpSettings.ProxySettings.INFERENCE_PROXY_PORT_KEY;
import static org.elasticsearch.xpack.inference.external.http.HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HttpSettingsTests extends ESTestCase {

    public void testProxy_IsNull_WhenNoHostIsConfigured() {
        assertThat(createHttpSettings(Settings.EMPTY).proxy(), nullValue());
    }

    public void testProxy_DefaultsToHttpSchemeAndItsDefaultPort() {
        var proxy = createHttpSettings(Settings.builder().put(INFERENCE_PROXY_HOST_KEY, "proxy.example.com").build()).proxy();

        assertThat(proxy.host(), is("proxy.example.com"));
        assertThat(proxy.scheme(), is(HttpSettings.ProxySettings.Scheme.HTTP));
        assertThat(proxy.port(), is(80));
    }

    public void testProxy_UsesConfiguredPortAndScheme() {
        var port = randomIntBetween(1, 0xFFFF);
        var settings = Settings.builder()
            .put(INFERENCE_PROXY_HOST_KEY, "proxy.example.com")
            .put(INFERENCE_PROXY_PORT_KEY, port)
            .put(INFERENCE_PROXY_SCHEME_KEY, "http")
            .build();

        var proxy = createHttpSettings(settings).proxy();

        assertThat(proxy.port(), is(port));
        assertThat(proxy.scheme().scheme(), is("http"));
    }

    public void testProxySchemeSetting_RejectsHttps() {
        var settings = Settings.builder().put(INFERENCE_PROXY_SCHEME_KEY, "https").build();

        var exception = expectThrows(IllegalArgumentException.class, () -> HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME.get(settings));

        assertThat(exception.getMessage(), containsString("only supports [http] but was [https]"));
        assertThat(exception.getMessage(), containsString("[https] proxies are not supported"));
    }

    public void testProxySchemeSetting_RejectsUnknownScheme() {
        var settings = Settings.builder().put(INFERENCE_PROXY_SCHEME_KEY, "socks").build();

        var exception = expectThrows(IllegalArgumentException.class, () -> HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME.get(settings));

        assertThat(exception.getMessage(), containsString("only supports [http] but was [socks]"));
    }

    public void testConstructor_RejectsHttpsProxyScheme() {
        var settings = Settings.builder()
            .put(INFERENCE_PROXY_HOST_KEY, "proxy.example.com")
            .put(INFERENCE_PROXY_SCHEME_KEY, "https")
            .build();

        var exception = expectThrows(IllegalArgumentException.class, () -> createHttpSettings(settings));

        assertThat(exception.getMessage(), containsString("only supports [http] but was [https]"));
    }

    public void testProxySchemeSetting_AcceptsHttp() {
        var settings = Settings.builder().put(INFERENCE_PROXY_SCHEME_KEY, "http").build();

        assertThat(HttpSettings.ProxySettings.INFERENCE_PROXY_SCHEME.get(settings), equalTo("http"));
    }

    public void testGetSettingsDefinitions_IncludesProxySettings() {
        var keys = HttpSettings.getSettingsDefinitions().stream().map(setting -> setting.getKey()).toList();

        assertThat(keys, hasItems(INFERENCE_PROXY_HOST_KEY, INFERENCE_PROXY_PORT_KEY, INFERENCE_PROXY_SCHEME_KEY));
    }

    private static HttpSettings createHttpSettings(Settings settings) {
        return new HttpSettings(settings, mockClusterService(settings));
    }
}
