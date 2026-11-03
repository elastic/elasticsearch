/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Nodes older than {@code esql_documents_found_and_values_loaded} (released 9.0) do not serialize
 * {@link DriverCompletionInfo}; their compute warnings arrive as {@code Warning} response headers in the
 * {@link ThreadContext}. These tests check that {@link ComputeResponse} and {@link DataNodeComputeResponse}
 * move those headers into {@link DriverCompletionInfo#warnings()}. The mixed-cluster and multi-cluster
 * csv-spec suites tolerate missing warnings on 9.0 to 9.3 nodes, so they cannot catch a regression here.
 */
public class ComputeResponseBwcWarningsTests extends ESTestCase {

    private static final TransportVersion BEFORE_COMPLETION_INFO = TransportVersionUtils.getPreviousVersion(
        TransportVersion.fromName("esql_documents_found_and_values_loaded")
    );
    private static final TransportVersion BEFORE_SHARD_LEVEL_RETRY = TransportVersionUtils.getPreviousVersion(
        TransportVersion.fromName("esql_retry_on_shard_level_failure")
    );

    public void testComputeResponseTakesWarningsFromOldNode() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        var bytes = serialize(new ComputeResponse(completionInfo()), BEFORE_COMPLETION_INFO);
        addOldNodeWarnings(threadContext);

        ComputeResponse response = new ComputeResponse(input(bytes, BEFORE_COMPLETION_INFO), threadContext);

        assertThat(response.getCompletionInfo().warnings(), contains("field [foo] is deprecated", "field [bar] is deprecated"));
        assertThat(warningHeaders(threadContext), is(empty()));
    }

    public void testDataNodeComputeResponseTakesWarningsFromOldNodeWithShardLevelRetry() throws IOException {
        assertDataNodeComputeResponseTakesWarnings(BEFORE_COMPLETION_INFO);
    }

    public void testDataNodeComputeResponseTakesWarningsFromOldNodeWithoutShardLevelRetry() throws IOException {
        assertDataNodeComputeResponseTakesWarnings(BEFORE_SHARD_LEVEL_RETRY);
    }

    public void testOldNodeWithoutWarnings() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        TransportVersion version = randomFrom(BEFORE_COMPLETION_INFO, BEFORE_SHARD_LEVEL_RETRY);
        var bytes = serialize(new DataNodeComputeResponse(completionInfo(), Map.of()), version);

        DataNodeComputeResponse response = new DataNodeComputeResponse(input(bytes, version), threadContext);

        assertThat(response.completionInfo().warnings(), is(empty()));
        assertThat(warningHeaders(threadContext), is(empty()));
    }

    private static void assertDataNodeComputeResponseTakesWarnings(TransportVersion version) throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        var bytes = serialize(new DataNodeComputeResponse(completionInfo(), Map.of()), version);
        addOldNodeWarnings(threadContext);

        DataNodeComputeResponse response = new DataNodeComputeResponse(input(bytes, version), threadContext);

        assertThat(response.completionInfo().warnings(), contains("field [foo] is deprecated", "field [bar] is deprecated"));
        assertThat(warningHeaders(threadContext), is(empty()));
    }

    private static DriverCompletionInfo completionInfo() {
        return new DriverCompletionInfo(0, 0, 0, 0, 0, 0, 0, 0, List.of(), List.of(), Map.of(), false, false, Set.of());
    }

    /**
     * The transport layer deposits the old node's response headers into the receiving thread's context
     * before the response is read.
     */
    private static void addOldNodeWarnings(ThreadContext threadContext) {
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("field [foo] is deprecated"));
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("field [bar] is deprecated"));
    }

    private static List<String> warningHeaders(ThreadContext threadContext) {
        return threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
    }

    private static BytesStreamOutput serialize(Writeable writeable, TransportVersion version) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(version);
        writeable.writeTo(out);
        return out;
    }

    private static StreamInput input(BytesStreamOutput bytes, TransportVersion version) throws IOException {
        StreamInput in = bytes.bytes().streamInput();
        in.setTransportVersion(version);
        return in;
    }
}
