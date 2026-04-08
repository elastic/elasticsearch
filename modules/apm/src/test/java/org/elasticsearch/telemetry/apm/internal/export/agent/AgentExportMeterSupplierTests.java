/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.agent;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class AgentExportMeterSupplierTests extends ESTestCase {

    /**
     * attemptFlushMetrics() before get() is a no-op: if the agent was never activated there are no
     * buffered metrics and sleeping would be wasteful (e.g. metrics were disabled for the lifetime of the node).
     */
    public void testAttemptFlushBeforeGetIsNoop() {
        List<Long> sleepCalls = new ArrayList<>();
        AgentExportMeterSupplier supplier = new AgentExportMeterSupplier(10_000L, sleepCalls::add);

        supplier.attemptFlushMetrics();

        assertThat(sleepCalls, empty());
    }

    /**
     * Once get() has been called the agent is active and may have buffered metrics, so
     * attemptFlushMetrics() must trigger the sleep to allow the agent to drain them.
     */
    public void testAttemptFlushAfterGetSleeps() {
        List<Long> sleepCalls = new ArrayList<>();
        AgentExportMeterSupplier supplier = new AgentExportMeterSupplier(10_000L, sleepCalls::add);

        supplier.get(); // activates the agent path
        supplier.attemptFlushMetrics();

        assertThat(sleepCalls, contains(10_000L));
    }
}
