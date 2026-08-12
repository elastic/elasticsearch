/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class DataNodeComputeResponseTests extends ESTestCase {

    /**
     * Reproduces the BWC warning loss in {@link DataNodeComputeResponse} after PR #155976.
     * <p>
     * When a new coordinator deserializes a {@code DataNodeComputeResponse} from an old data node
     * that does not support the {@code esql_driver_warnings} wire field, warnings that the old
     * node sent as transport response headers (the pre-PR mechanism) are silently discarded.
     * {@link DriverCompletionInfo#readFrom} sets {@code warnings = Set.of()} for old versions,
     * and there is no fallback to read them from the {@link ThreadContext} — unlike
     * {@code BatchExchangeStatusResponse} which correctly recovers them.
     * <p>
     * This causes intermittent failures in mixed-cluster BWC tests (e.g.
     * {@code MixedClusterEsqlSpec*IT}) whenever a query that generates warnings is routed to an
     * old data node.
     */
    public void testWarningsRecoveredFromThreadContextWhenOldVersion() throws IOException {
        var warningTexts = List.of(
            "Line 1:9: evaluation of [x] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:9: java.lang.IllegalArgumentException: single-value function encountered multi-value"
        );
        var completionInfoWithWarnings = new DriverCompletionInfo(
            10,
            10,
            5,
            0,
            0,
            0,
            List.of(),
            List.of(),
            Map.of(),
            false,
            new LinkedHashSet<>(warningTexts)
        );
        var response = new DataNodeComputeResponse(completionInfoWithWarnings, Map.of());

        // Use a transport version just before esql_driver_warnings — supports the full
        // DriverCompletionInfo wire format but not the warnings field.
        var oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);

        // Serialize with the old version: warnings are NOT written to the wire.
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        response.writeTo(out);

        // Before deserializing, set up ThreadContext with Warning response headers — this is
        // exactly what the transport framework does when receiving a response from an old node
        // that emitted warnings via HeaderWarning.addWarning (the pre-PR mechanism).
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        for (String warning : warningTexts) {
            threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning(warning));
        }

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        var deserialized = new DataNodeComputeResponse(in);

        // The deserialized response should have recovered the warnings from the ThreadContext,
        // just as BatchExchangeStatusResponse does.
        assertThat(deserialized.completionInfo().warnings(), containsInAnyOrder(warningTexts.toArray()));
    }

    /**
     * Confirms that the current code drops warnings from an old node: the round-trip produces an
     * empty warnings set even though the warnings are present in the ThreadContext.
     * This is the symptom that the fix must eliminate.
     */
    public void testWarningsLostFromOldVersion() throws IOException {
        var warningTexts = List.of(
            "Line 1:9: evaluation of [x] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:9: java.lang.IllegalArgumentException: single-value function encountered multi-value"
        );
        var completionInfoWithWarnings = new DriverCompletionInfo(
            10,
            10,
            5,
            0,
            0,
            0,
            List.of(),
            List.of(),
            Map.of(),
            false,
            new LinkedHashSet<>(warningTexts)
        );
        var response = new DataNodeComputeResponse(completionInfoWithWarnings, Map.of());

        var oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        response.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        var deserialized = new DataNodeComputeResponse(in);

        // Confirms the current broken behavior: warnings are empty.
        assertThat(deserialized.completionInfo().warnings(), empty());
    }
}
