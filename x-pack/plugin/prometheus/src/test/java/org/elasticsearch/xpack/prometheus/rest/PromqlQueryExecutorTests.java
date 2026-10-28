/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PromqlQueryExecutorTests extends ESTestCase {

    public void testParseTimeoutSeconds() {
        assertThat(PromqlQueryExecutor.parseTimeout("30"), equalTo(TimeValue.timeValueSeconds(30)));
    }

    public void testParseTimeoutDurationLiteral() {
        assertThat(PromqlQueryExecutor.parseTimeout("30d"), equalTo(TimeValue.timeValueDays(30)));
        assertThat(PromqlQueryExecutor.parseTimeout("500ms"), equalTo(TimeValue.timeValueMillis(500)));
        assertThat(PromqlQueryExecutor.parseTimeout("2m"), equalTo(TimeValue.timeValueMinutes(2)));
        assertThat(PromqlQueryExecutor.parseTimeout("1m30s"), equalTo(TimeValue.timeValueSeconds(90)));
    }

    public void testParseTimeoutInvalid() {
        for (String value : new String[] { "soon", "1x", "1.5", "s30", "30s1m" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PromqlQueryExecutor.parseTimeout(value));
            assertThat(e.getMessage(), equalTo("invalid parameter \"timeout\": cannot parse \"" + value + "\" to a valid duration"));
        }
    }

    public void testParseTimeoutNotPositive() {
        for (String value : new String[] { "0", "-1", "0s" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PromqlQueryExecutor.parseTimeout(value));
            assertThat(e.getMessage(), equalTo("invalid parameter \"timeout\": must be positive, got \"" + value + "\""));
        }
    }

    public void testResolveTimeoutDefaultsToMax() {
        TimeValue max = TimeValue.timeValueMinutes(2);
        assertThat(PromqlQueryExecutor.resolveTimeout(request(Map.of()), max), equalTo(max));
        assertThat(PromqlQueryExecutor.resolveTimeout(request(Map.of("timeout", "")), max), equalTo(max));
    }

    public void testResolveTimeoutIsCappedByMax() {
        TimeValue max = TimeValue.timeValueMinutes(2);
        assertThat(PromqlQueryExecutor.resolveTimeout(request(Map.of("timeout", "30s")), max), equalTo(TimeValue.timeValueSeconds(30)));
        assertThat(PromqlQueryExecutor.resolveTimeout(request(Map.of("timeout", "1h")), max), equalTo(max));
    }

    public void testResolveTimeoutWithoutMax() {
        assertThat(PromqlQueryExecutor.resolveTimeout(request(Map.of()), TimeValue.MINUS_ONE), nullValue());
        assertThat(
            PromqlQueryExecutor.resolveTimeout(request(Map.of("timeout", "1h")), TimeValue.MINUS_ONE),
            equalTo(TimeValue.timeValueHours(1))
        );
    }

    private static RestRequest request(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
    }
}
