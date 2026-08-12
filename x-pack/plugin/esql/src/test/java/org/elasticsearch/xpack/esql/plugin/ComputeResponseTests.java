/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ComputeResponseTests extends ESTestCase {

    /**
     * Verifies that warnings are recovered from {@link ThreadContext} response headers when
     * deserializing a {@link ComputeResponse} from an old remote cluster node that does not
     * support the {@code esql_driver_warnings} wire field.
     * <p>
     * Old nodes send warnings as transport response headers; the transport layer deposits them
     * into the current thread's context before the response constructor is called. The
     * {@link ThreadContext}-aware constructor recovers them, matching the pattern used by
     * {@code BatchExchangeStatusResponse}.
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
        var response = new ComputeResponse(completionInfoWithWarnings, TimeValue.timeValueMillis(100), 5, 5, 0, 0, List.of());

        // Use a transport version just before esql_driver_warnings.
        var oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);

        // Serialize with the old version: warnings are NOT written to the wire.
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        response.writeTo(out);

        // Set up ThreadContext with Warning response headers — simulating what the transport
        // framework does when receiving a response from an old remote cluster node.
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        for (String warning : warningTexts) {
            threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning(warning));
        }

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        var deserialized = new ComputeResponse(in, threadContext);

        assertThat(deserialized.getCompletionInfo().warnings(), containsInAnyOrder(warningTexts.toArray()));
    }
}
