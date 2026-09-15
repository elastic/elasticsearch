/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest;

import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.regex.Matcher;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESRestTestCaseTests extends ESTestCase {

    // Minimal concrete subclass — ESRestTestCase declares no abstract methods
    static class MinimalRestTestCase extends ESRestTestCase {}

    private MinimalRestTestCase subject;

    @Before
    public void setUpClusterAvailabilityFields() {
        subject = new MinimalRestTestCase();
        ESRestTestCase.adminClient = null;
        ESRestTestCase.clusterUnavailable = false;
    }

    @After
    public void tearDownClusterAvailabilityFields() {
        ESRestTestCase.adminClient = null;
        ESRestTestCase.clusterUnavailable = false;
    }

    /** Runs the clusterDeadRule around a statement that always throws the given throwable. */
    private void evaluateRule(Throwable toThrow) throws Throwable {
        Statement failing = new Statement() {
            @Override
            public void evaluate() throws Throwable {
                throw toThrow;
            }
        };
        Description description = Description.createTestDescription(MinimalRestTestCase.class, "testMethod");
        subject.clusterDeadRule.apply(failing, description).evaluate();
    }

    public void testAssumptionViolationPassesThroughWithoutSettingFlag() throws Throwable {
        AssumptionViolatedException assumption = new AssumptionViolatedException("skipped");
        AssumptionViolatedException thrown = expectThrows(AssumptionViolatedException.class, () -> evaluateRule(assumption));
        assertSame(assumption, thrown);
        assertFalse(ESRestTestCase.clusterUnavailable);
    }

    public void testNullClientSetsUnavailableWithOriginalFailureSuppressed() throws Throwable {
        RuntimeException original = new RuntimeException("test failure");
        AssertionError thrown = expectThrows(AssertionError.class, () -> evaluateRule(original));
        assertThat(thrown.getMessage(), containsString("initialization failed"));
        assertNull(thrown.getCause());
        assertSame(original, thrown.getSuppressed()[0]);
        assertTrue(ESRestTestCase.clusterUnavailable);
    }

    public void testSuccessfulPingRethrownOriginalFailure() throws Throwable {
        // RestClient requires a live HTTP server to construct; mock to control ping response
        RestClient mockClient = mock(RestClient.class);
        // performRequest returns normally — ping succeeds, cluster is alive
        ESRestTestCase.adminClient = mockClient;

        RuntimeException original = new RuntimeException("test failure");
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> evaluateRule(original));
        assertSame(original, thrown);
        assertFalse(ESRestTestCase.clusterUnavailable);
    }

    public void testIOExceptionPingSetsUnreachable() throws Throwable {
        // RestClient requires a live HTTP server to construct; mock to control ping response
        RestClient mockClient = mock(RestClient.class);
        IOException pingFailure = new IOException("connection refused");
        when(mockClient.performRequest(any(Request.class))).thenThrow(pingFailure);
        ESRestTestCase.adminClient = mockClient;

        RuntimeException original = new RuntimeException("test failure");
        AssertionError thrown = expectThrows(AssertionError.class, () -> evaluateRule(original));
        assertEquals("Test cluster is unreachable", thrown.getMessage());
        assertSame(pingFailure, thrown.getCause());
        assertSame(original, thrown.getSuppressed()[0]);
        assertTrue(ESRestTestCase.clusterUnavailable);
    }

    public void testUncheckExceptionPingSetsUnreachable() throws Throwable {
        // RestClient requires a live HTTP server to construct; mock to control ping response
        RestClient mockClient = mock(RestClient.class);
        IllegalStateException pingFailure = new IllegalStateException("client closed");
        when(mockClient.performRequest(any(Request.class))).thenThrow(pingFailure);
        ESRestTestCase.adminClient = mockClient;

        RuntimeException original = new RuntimeException("test failure");
        AssertionError thrown = expectThrows(AssertionError.class, () -> evaluateRule(original));
        assertEquals("Test cluster is unreachable", thrown.getMessage());
        assertSame(pingFailure, thrown.getCause());
        assertSame(original, thrown.getSuppressed()[0]);
        assertTrue(ESRestTestCase.clusterUnavailable);
    }

    public void testResponseExceptionPingSetsBadState() throws Throwable {
        // RestClient requires a live HTTP server to construct; mock to control ping response
        RestClient mockClient = mock(RestClient.class);
        ResponseException pingFailure = mockResponseException();
        when(mockClient.performRequest(any(Request.class))).thenThrow(pingFailure);
        ESRestTestCase.adminClient = mockClient;

        RuntimeException original = new RuntimeException("test failure");
        AssertionError thrown = expectThrows(AssertionError.class, () -> evaluateRule(original));
        assertEquals("Test cluster is in a bad state", thrown.getMessage());
        assertSame(pingFailure, thrown.getCause());
        assertSame(original, thrown.getSuppressed()[0]);
        assertTrue(ESRestTestCase.clusterUnavailable);
    }

    public void testRuleSkipsWhenClusterUnavailable() throws Throwable {
        ESRestTestCase.clusterUnavailable = true;
        expectThrows(AssumptionViolatedException.class, () -> evaluateRule(new RuntimeException("would fail")));
        // flag unchanged — skip, not a new failure
        assertTrue(ESRestTestCase.clusterUnavailable);
    }

    public void testPreviousFailureSkipsRemainingReflectsFlag() {
        assertFalse(subject.previousFailureSkipsRemaining());
        ESRestTestCase.clusterUnavailable = true;
        assertTrue(subject.previousFailureSkipsRemaining());
    }

    public void testResetClusterUnavailableClearsFlag() {
        ESRestTestCase.clusterUnavailable = true;
        ESRestTestCase.resetClusterUnavailable();
        assertFalse(ESRestTestCase.clusterUnavailable);
    }

    // ResponseException requires a Response, which requires a live HTTP exchange; mock the chain
    private static ResponseException mockResponseException() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getRequestLine()).thenReturn(requestLine);
        when(requestLine.getMethod()).thenReturn("HEAD");
        when(requestLine.getUri()).thenReturn("/");
        when(response.getHost()).thenReturn(new HttpHost("localhost", 9200));
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.toString()).thenReturn("HTTP/1.1 503 Service Unavailable");
        when(response.hasWarnings()).thenReturn(false);
        when(response.getEntity()).thenReturn(null);
        return new ResponseException(response);
    }

    public void testIgnoreMatchMultipleTemplatesPattern() {
        String input = "index [test_index] matches multiple legacy templates [global, prevent-bwc-deprecation-template], "
            + "composable templates will only match a single template";
        Matcher matcher = ESRestTestCase.CREATE_INDEX_MULTIPLE_MATCHING_TEMPLATES.matcher(input);
        assertThat(matcher.matches(), is(true));
        assertThat(matcher.group(1), equalTo("test_index"));
        assertThat(matcher.group(2), equalTo("global, prevent-bwc-deprecation-template"));

        input = "index template [1] has index patterns [logs-*] matching patterns from existing older templates [global] "
            + "with patterns (global => [*]); this template [1] will take precedence during new index creation";
        matcher = ESRestTestCase.PUT_TEMPLATE_MULTIPLE_MATCHING_TEMPLATES.matcher(input);
        assertThat(matcher.matches(), is(true));
        assertThat(matcher.group(1), equalTo("1"));
        assertThat(matcher.group(2), equalTo("logs-*"));
        assertThat(matcher.group(3), equalTo("global"));
        assertThat(matcher.group(4), equalTo("global => [*]"));
        assertThat(matcher.group(5), equalTo("1"));
    }

    public void testListedTransientStatusShouldBeRetryable() {
        assertThat(ESRestTestCase.isRetryableStatus(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND), is(true));
        assertThat(
            ESRestTestCase.isRetryableStatus(
                RestStatus.SERVICE_UNAVAILABLE.getStatus(),
                RestStatus.NOT_FOUND,
                RestStatus.SERVICE_UNAVAILABLE
            ),
            is(true)
        );
    }

    public void testUnlistedStatusShouldNotBeRetryable() {
        assertThat(
            ESRestTestCase.isRetryableStatus(
                RestStatus.INTERNAL_SERVER_ERROR.getStatus(),
                RestStatus.NOT_FOUND,
                RestStatus.SERVICE_UNAVAILABLE
            ),
            is(false)
        );
    }

    public void testEmptyRetryableSetShouldNotBeRetryable() {
        assertThat(ESRestTestCase.isRetryableStatus(RestStatus.NOT_FOUND.getStatus()), is(false));
    }

}
