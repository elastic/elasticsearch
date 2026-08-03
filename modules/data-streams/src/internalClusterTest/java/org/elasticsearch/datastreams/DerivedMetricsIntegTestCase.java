/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.concurrent.TimeUnit;

/**
 * Base for the derived metrics integration tests, which all run with a short flush interval so that intervals close inside a test.
 *
 * <p>That short interval is also what made them intermittently fail on the way out. Emission is deliberately fire and forget: a bucket is
 * flushed on the derived metrics threadpool and the bulk it produces is sent without anyone waiting for it. When a test ends, the cluster
 * starts closing every index, including the destination the feature is still writing into, and a shard whose bulk has not finished cannot
 * release its lock — which the test framework reports as {@code Shard [...] is still locked after 5 sec waiting}.
 *
 * <p>Nothing is wrong with the feature there: a node shutting down while a bulk is in flight is ordinary, and the bulk either lands or
 * fails. What is wrong is the test ending while work it started is still running. So each test waits for the feature to go quiet before
 * the cluster is torn down, rather than the failure being muted or the assertion relaxed.
 */
public abstract class DerivedMetricsIntegTestCase extends ESIntegTestCase {

    @After
    public void waitUntilDerivedMetricsAreQuiet() throws Exception {
        assertBusy(() -> {
            int inFlight = 0;
            int buffered = 0;
            for (DerivedMetricsService service : internalCluster().getInstances(DerivedMetricsService.class)) {
                inFlight += service.inFlightDocuments();
                buffered += service.bufferedSeries();
            }
            // Both, and for different reasons. Nothing in flight means no write is outstanding; nothing buffered means the next scheduled
            // flush cannot start one — and it is that second write that does the damage, because a late emission creates the destination
            // data stream and allocates its shard while the cluster is already being torn down.
            assertEquals("derived metrics emission is still in flight as the test ends", 0, inFlight);
            assertEquals("derived metrics still has series buffered that a later flush would emit", 0, buffered);
        }, 30, TimeUnit.SECONDS);
    }
}
