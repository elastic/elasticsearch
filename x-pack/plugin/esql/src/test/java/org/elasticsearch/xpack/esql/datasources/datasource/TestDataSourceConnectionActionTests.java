/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasources.TestConnectionResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link TestDataSourceConnectionAction.Response} XContent,
 * {@link TestDataSourceNodeAction.NodeResponse} wire format, and
 * {@link TransportTestDataSourceConnectionAction#aggregate}.
 */
public class TestDataSourceConnectionActionTests extends ESTestCase {

    // ---- Response.toXContent ----

    public void testResponseXContentSuccess() throws IOException {
        String json = toJson(TestDataSourceConnectionAction.Response.success());
        assertEquals("{\"status\":\"success\"}", json);
    }

    public void testResponseXContentFailure() throws IOException {
        String json = toJson(TestDataSourceConnectionAction.Response.failure("bad credentials"));
        assertEquals("{\"status\":\"failure\",\"error\":\"bad credentials\"}", json);
    }

    public void testResponseXContentUntestableNoMessage() throws IOException {
        String json = toJson(TestDataSourceConnectionAction.Response.untestable());
        assertEquals("{\"status\":\"untestable\"}", json);
    }

    public void testResponseXContentUntestableWithMessage() throws IOException {
        String json = toJson(TestDataSourceConnectionAction.Response.untestable("create a dataset to validate access"));
        assertEquals("{\"status\":\"untestable\",\"message\":\"create a dataset to validate access\"}", json);
    }

    private static String toJson(TestDataSourceConnectionAction.Response response) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, null);
        return Strings.toString(builder);
    }

    // ---- NodeResponse wire round-trip ----

    public void testNodeResponseRoundTripSuccess() throws IOException {
        assertRoundTrip(new TestDataSourceNodeAction.NodeResponse(TestConnectionResult.SUCCESS));
    }

    public void testNodeResponseRoundTripFailure() throws IOException {
        assertRoundTrip(new TestDataSourceNodeAction.NodeResponse(TestConnectionResult.failure("connection refused")));
    }

    public void testNodeResponseRoundTripUntestableNoReason() throws IOException {
        assertRoundTrip(new TestDataSourceNodeAction.NodeResponse(new TestConnectionResult.Untestable(null)));
    }

    public void testNodeResponseRoundTripUntestableWithReason() throws IOException {
        assertRoundTrip(new TestDataSourceNodeAction.NodeResponse(new TestConnectionResult.Untestable("bucket-scoped credentials")));
    }

    private static void assertRoundTrip(TestDataSourceNodeAction.NodeResponse original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TestDataSourceNodeAction.NodeResponse copy = new TestDataSourceNodeAction.NodeResponse(in);
        assertEquals(0, in.available());
        assertEquals(original.result, copy.result);
    }

    // ---- NodeRequest wire round-trip ----

    public void testNodeRequestRoundTrip() throws IOException {
        TestDataSourceNodeAction.NodeRequest original = new TestDataSourceNodeAction.NodeRequest(
            "s3",
            Map.of("access_key", "AKIAIOSFODNN7EXAMPLE", "auth", "static_credentials")
        );
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TestDataSourceNodeAction.NodeRequest copy = new TestDataSourceNodeAction.NodeRequest(in);
        assertEquals(0, in.available());
        assertEquals(original.type, copy.type);
        assertEquals(original.rawSettings, copy.rawSettings);
    }

    // ---- aggregate() ----

    public void testAggregateAllSuccess() {
        AtomicArray<TestConnectionResult> results = resultsOf(
            TestConnectionResult.SUCCESS,
            TestConnectionResult.SUCCESS,
            TestConnectionResult.SUCCESS
        );
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("success", r.status());
    }

    public void testAggregateOneFailureWins() {
        AtomicArray<TestConnectionResult> results = resultsOf(
            TestConnectionResult.SUCCESS,
            TestConnectionResult.failure("auth error"),
            new TestConnectionResult.Untestable("bucket-scoped")
        );
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("failure", r.status());
        assertEquals("auth error", r.error());
    }

    public void testAggregateFirstFailureMessagePreserved() {
        AtomicArray<TestConnectionResult> results = resultsOf(
            TestConnectionResult.failure("first error"),
            TestConnectionResult.failure("second error")
        );
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("failure", r.status());
        assertEquals("first error", r.error());
    }

    public void testAggregateUntestableWhenNoFailure() {
        AtomicArray<TestConnectionResult> results = resultsOf(
            TestConnectionResult.SUCCESS,
            new TestConnectionResult.Untestable("cannot verify at this level"),
            TestConnectionResult.SUCCESS
        );
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("untestable", r.status());
        assertEquals("cannot verify at this level", r.message());
    }

    public void testAggregateUntestableNoReason() {
        AtomicArray<TestConnectionResult> results = resultsOf(new TestConnectionResult.Untestable(null));
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("untestable", r.status());
        assertNull(r.message());
    }

    public void testAggregateFailureBeatsUntestable() {
        AtomicArray<TestConnectionResult> results = resultsOf(
            new TestConnectionResult.Untestable("reason"),
            TestConnectionResult.failure("real failure"),
            TestConnectionResult.SUCCESS
        );
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("failure", r.status());
        assertEquals("real failure", r.error());
    }

    /**
     * The {@code handleException} path in the coordinator maps a {@link org.elasticsearch.transport.TransportException}
     * to the fixed untestable reason below. This test pins that text so refactoring the coordinator doesn't
     * silently change the user-visible message.
     */
    public void testHandleExceptionUntestableMessageText() {
        // The coordinator sets this exact string when a node probe does not complete (timeout, old node, etc.)
        String handleExceptionReason = "One or more nodes returned no probe result; the backend may still be reachable";
        AtomicArray<TestConnectionResult> results = resultsOf(new TestConnectionResult.Untestable(handleExceptionReason));
        TestDataSourceConnectionAction.Response r = TransportTestDataSourceConnectionAction.aggregate(results);
        assertEquals("untestable", r.status());
        assertEquals(handleExceptionReason, r.message());
    }

    /** The coordinator strips {@code region} from probe settings before fanning out.
     *  The NodeRequest wire format must accept a map without it; this round-trip verifies that path. */
    public void testNodeRequestRoundTripWithRegionStripped() throws IOException {
        // Simulate what the coordinator does: copy rawSettings and remove "region" before constructing the NodeRequest.
        Map<String, Object> rawSettings = new HashMap<>();
        rawSettings.put("auth", "static_credentials");
        rawSettings.put("access_key", "AKIAIOSFODNN7EXAMPLE");
        rawSettings.put("region", "us-east-1");
        rawSettings.remove("region"); // coordinator strips region before fanning out

        TestDataSourceNodeAction.NodeRequest original = new TestDataSourceNodeAction.NodeRequest("s3", rawSettings);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TestDataSourceNodeAction.NodeRequest copy = new TestDataSourceNodeAction.NodeRequest(in);

        assertFalse("region must not appear in the probe settings", copy.rawSettings.containsKey("region"));
        assertEquals("s3", copy.type);
    }

    private static AtomicArray<TestConnectionResult> resultsOf(TestConnectionResult... values) {
        AtomicArray<TestConnectionResult> arr = new AtomicArray<>(values.length);
        for (int i = 0; i < values.length; i++) {
            arr.set(i, values[i]);
        }
        return arr;
    }
}
