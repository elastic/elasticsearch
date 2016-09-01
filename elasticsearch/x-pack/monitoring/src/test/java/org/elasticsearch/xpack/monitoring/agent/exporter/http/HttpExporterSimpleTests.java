/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.exporter.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporter;
import org.elasticsearch.xpack.ssl.SSLService;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link HttpExporter}.
 */
public class HttpExporterSimpleTests extends ESTestCase {

    private final Environment environment = mock(Environment.class);

    public void testExporterWithBlacklistedHeaders() {
        final String blacklistedHeader = randomFrom(HttpExporter.BLACKLISTED_HEADERS);
        final String expected = "[" + blacklistedHeader + "] cannot be overwritten via [xpack.monitoring.exporters._http.headers]";
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters._http.host", "http://localhost:9200")
                .put("xpack.monitoring.exporters._http.headers.abc", "xyz")
                .put("xpack.monitoring.exporters._http.headers." + blacklistedHeader, "value should not matter");

        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.headers.xyz", "abc");
        }

        final Exporter.Config config = createConfig("_http", builder.build());

        final SettingsException exception = expectThrows(SettingsException.class, () -> {
            new HttpExporter(config, environment, new SSLService(builder.build(), environment));
        });

        assertThat(exception.getMessage(), equalTo(expected));
    }

    public void testExporterWithEmptyHeaders() {
        final String name = randomFrom("abc", "ABC", "X-Flag");
        final String expected = "headers must have values, missing for setting [xpack.monitoring.exporters._http.headers." + name + "]";
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
                .put("xpack.monitoring.exporters._http.host", "localhost:9200")
                .put("xpack.monitoring.exporters._http.headers." + name, "");

        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.headers.xyz", "abc");
        }

        final Exporter.Config config = createConfig("_http", builder.build());

        final SettingsException exception = expectThrows(SettingsException.class, () -> {
            new HttpExporter(config, environment, new SSLService(builder.build(), environment));
        });

        assertThat(exception.getMessage(), equalTo(expected));
    }

    public void testExporterWithMissingHost() {
        // forgot host!
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE);

        if (randomBoolean()) {
            builder.put("xpack.monitoring.exporters._http.host", "");
        } else if (randomBoolean()) {
            builder.putArray("xpack.monitoring.exporters._http.host");
        } else if (randomBoolean()) {
            builder.putNull("xpack.monitoring.exporters._http.host");
        }

        final Exporter.Config config = createConfig("_http", builder.build());

        final SettingsException exception = expectThrows(SettingsException.class, () -> {
            new HttpExporter(config, environment, new SSLService(builder.build(), environment));
        });

        assertThat(exception.getMessage(), equalTo("missing required setting [xpack.monitoring.exporters._http.host]"));
    }

    public void testExporterWithInvalidHost() {
        final String invalidHost = randomFrom("://localhost:9200", "gopher!://xyz.my.com");

        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE);

        // sometimes add a valid URL with it
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.putArray("xpack.monitoring.exporters._http.host", "localhost:9200", invalidHost);
            } else {
                builder.putArray("xpack.monitoring.exporters._http.host", invalidHost, "localhost:9200");
            }
        } else {
            builder.put("xpack.monitoring.exporters._http.host", invalidHost);
        }

        final Exporter.Config config = createConfig("_http", builder.build());

        final SettingsException exception = expectThrows(SettingsException.class, () -> {
            new HttpExporter(config, environment, new SSLService(builder.build(), environment));
        });

        assertThat(exception.getMessage(), equalTo("[xpack.monitoring.exporters._http.host] invalid host: [" + invalidHost + "]"));
    }

    public void testExporterWithHostOnly() {
        final Settings.Builder builder = Settings.builder()
                .put("xpack.monitoring.exporters._http.type", "http")
                .put("xpack.monitoring.exporters._http.host", "http://localhost:9200");

        final Exporter.Config config = createConfig("_http", builder.build());

        new HttpExporter(config, environment, new SSLService(builder.build(), environment));
    }

    /**
     * Create the {@link Exporter.Config} with the given name, and select those settings from {@code settings}.
     *
     * @param name The name of the exporter.
     * @param settings The settings to select the exporter's settings from
     * @return Never {@code null}.
     */
    private static Exporter.Config createConfig(String name, Settings settings) {
        return new Exporter.Config(name, HttpExporter.TYPE, Settings.EMPTY, settings.getAsSettings("xpack.monitoring.exporters." + name));
    }

}
