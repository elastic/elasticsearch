/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;

import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;

abstract class AbstractAuthenticatorTests extends ESTestCase {

    protected void assertSingleSuccessAuthMetric(
        TestTelemetryPlugin telemetryPlugin,
        SecurityMetricType metricType,
        Map<String, Object> attributes
    ) {
        List<Measurement> successMetrics = telemetryPlugin.getLongCounterMeasurement(metricType.successMetricInfo().name());
        assertThat(successMetrics.size(), equalTo(1));
        assertThat(successMetrics.get(0).getLong(), equalTo(1L));
        assertThat(successMetrics.get(0).attributes(), equalTo(attributes));
    }

    protected void assertSingleFailedAuthMetric(
        TestTelemetryPlugin telemetryPlugin,
        SecurityMetricType metricType,
        Map<String, Object> attributes
    ) {
        List<Measurement> failuresMetrics = telemetryPlugin.getLongCounterMeasurement(metricType.failuresMetricInfo().name());
        assertThat(failuresMetrics.size(), equalTo(1));
        assertThat(failuresMetrics.get(0).getLong(), equalTo(1L));
        assertThat(failuresMetrics.get(0).attributes(), equalTo(attributes));
    }

    protected void assertAuthenticationTimeMetric(
        TestTelemetryPlugin telemetryPlugin,
        SecurityMetricType metricType,
        long expectedAuthenticationTime,
        Map<String, Object> attributes
    ) {
        List<Measurement> authTimeMetrics = telemetryPlugin.getLongHistogramMeasurement(metricType.timeMetricInfo().name());
        assertThat(authTimeMetrics.size(), equalTo(1));
        assertThat(authTimeMetrics.get(0).getLong(), equalTo(expectedAuthenticationTime));
        assertThat(authTimeMetrics.get(0).attributes(), equalTo(attributes));
    }

    protected void assertZeroSuccessAuthMetrics(TestTelemetryPlugin telemetryPlugin, SecurityMetricType metricType) {
        List<Measurement> successMetrics = telemetryPlugin.getLongCounterMeasurement(metricType.successMetricInfo().name());
        assertThat(successMetrics.size(), equalTo(0));
    }

    protected void assertZeroFailedAuthMetrics(TestTelemetryPlugin telemetryPlugin, SecurityMetricType metricType) {
        List<Measurement> failuresMetrics = telemetryPlugin.getLongCounterMeasurement(metricType.failuresMetricInfo().name());
        assertThat(failuresMetrics.size(), equalTo(0));
    }

    static class TestNanoTimeSupplier implements LongSupplier {

        private long currentTime;

        TestNanoTimeSupplier(long initialTime) {
            this.currentTime = initialTime;
        }

        public void advanceTime(long timeToAdd) {
            this.currentTime += timeToAdd;
        }

        @Override
        public long getAsLong() {
            return currentTime;
        }
    }

}
