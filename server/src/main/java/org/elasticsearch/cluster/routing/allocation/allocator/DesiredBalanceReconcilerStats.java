/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

public class DesiredBalanceReconcilerStats {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceReconcilerStats.class);

    private final FrequencyCappedAction logReconciliationMetrics;

    public DesiredBalanceReconcilerStats(ThreadPool threadPool) {
        this.logReconciliationMetrics = new FrequencyCappedAction(threadPool);
        this.logReconciliationMetrics.setMinInterval(TimeValue.timeValueMinutes(30));
    }

    public void logUndesiredAllocationsMetrics(int allAllocations, int undesiredAllocations) {
        logReconciliationMetrics.maybeExecute(
            () -> logger.debug(
                new ESLogMessage("DesiredBalanceReconciler stats") //
                    .field(
                        "allocator.desired_balance.reconciliation.undesired_allocations_fraction",
                        (double) undesiredAllocations / allAllocations
                    )
                    .field("allocator.desired_balance.reconciliation.undesired_allocations", undesiredAllocations)
                    .field("allocator.desired_balance.reconciliation.total_allocations", allAllocations)
            )
        );
    }
}
