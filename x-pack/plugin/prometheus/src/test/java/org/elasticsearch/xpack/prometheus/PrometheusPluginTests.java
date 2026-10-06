/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.prometheus.PrometheusPlugin.PROMETHEUS_QUERY_TIMEOUT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PrometheusPluginTests extends ESTestCase {

    public void testQueryTimeoutDefault() {
        assertThat(PROMETHEUS_QUERY_TIMEOUT.get(Settings.EMPTY), equalTo(TimeValue.timeValueMinutes(2)));
    }

    public void testQueryTimeoutAcceptsPositiveAndMinusOne() {
        assertThat(PROMETHEUS_QUERY_TIMEOUT.get(timeoutSettings("30s")), equalTo(TimeValue.timeValueSeconds(30)));
        assertThat(PROMETHEUS_QUERY_TIMEOUT.get(timeoutSettings("-1")), equalTo(TimeValue.MINUS_ONE));
    }

    public void testQueryTimeoutRejectsZero() {
        for (String value : new String[] { "0", "0s", "0ms" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> PROMETHEUS_QUERY_TIMEOUT.get(timeoutSettings(value))
            );
            assertThat(e.getMessage(), containsString("must be positive or -1 to disable the timeout"));
        }
    }

    private static Settings timeoutSettings(String value) {
        return Settings.builder().put(PROMETHEUS_QUERY_TIMEOUT.getKey(), value).build();
    }
}
