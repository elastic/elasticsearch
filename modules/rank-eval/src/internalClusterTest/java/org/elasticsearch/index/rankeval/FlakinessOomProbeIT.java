/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.test.ESIntegTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

/**
 * Intentional agent-OOM probe for PR #151713 (flakiness-detection observability). NOT FOR MERGE.
 *
 * <p>This test is deterministically skipped under the regular build (where {@code tests.iters} defaults to 1) so it
 * never reddens normal CI. Under the flakiness-detection runner it allocates unbounded native (off-heap) memory until
 * the kernel OOM-killer terminates the worker process, which the pipeline records as an {@code infra_fail} outcome for
 * the integ batch step.
 *
 * <p>Native {@link Arena} memory is used deliberately: it is bounded by neither {@code -Xmx} nor
 * {@code MaxDirectMemorySize} and bypasses Elasticsearch's circuit breakers, so it grows RSS until the kernel kills the
 * process rather than throwing an in-JVM {@link OutOfMemoryError} (which would instead be recorded as a real test
 * failure and classified as {@code flaky_detected}).
 */
public class FlakinessOomProbeIT extends ESIntegTestCase {

    public void testIntentionalAgentOom() {
        assumeTrue("only runs under the flakiness-detection high-iteration run", Integer.getInteger("tests.iters", 1) > 1);

        // Never closed: keep every segment resident until the kernel kills this process.
        Arena arena = Arena.ofShared();
        List<MemorySegment> held = new ArrayList<>();
        long chunk = 1L << 30; // 1 GiB
        while (true) {
            MemorySegment segment = arena.allocate(chunk);
            // Touch one byte per page so the pages become resident and count against the process RSS.
            for (long offset = 0; offset < chunk; offset += 4096) {
                segment.set(ValueLayout.JAVA_BYTE, offset, (byte) 1);
            }
            held.add(segment);
        }
    }
}
