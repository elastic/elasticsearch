/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceStatsTests extends AbstractWireSerializingTestCase<DesiredBalanceStats> {

    @Override
    protected Writeable.Reader<DesiredBalanceStats> instanceReader() {
        return DesiredBalanceStats::readFrom;
    }

    @Override
    protected DesiredBalanceStats createTestInstance() {
        return randomDesiredBalanceStats();
    }

    public static DesiredBalanceStats randomDesiredBalanceStats() {
        return new DesiredBalanceStats(
            randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected DesiredBalanceStats mutateInstance(DesiredBalanceStats instance) {
        return switch (between(0, 11)) {
            case 0 -> new DesiredBalanceStats(
                randomValueOtherThan(instance.lastConvergedIndex(), ESTestCase::randomNonNegativeLong),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 1 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                randomValueOtherThan(instance.computationActive(), ESTestCase::randomBoolean),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 2 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                randomValueOtherThan(instance.computationSubmitted(), ESTestCase::randomNonNegativeLong),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 3 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                randomValueOtherThan(instance.computationExecuted(), ESTestCase::randomNonNegativeLong),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 4 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                randomValueOtherThan(instance.computationConverged(), ESTestCase::randomNonNegativeLong),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 5 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                randomValueOtherThan(instance.computationIterations(), ESTestCase::randomNonNegativeLong),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 6 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                randomValueOtherThan(instance.computedShardMovements(), ESTestCase::randomNonNegativeLong),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 7 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                randomValueOtherThan(instance.cumulativeComputationTime(), ESTestCase::randomNonNegativeLong),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 8 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                randomValueOtherThan(instance.cumulativeReconciliationTime(), ESTestCase::randomNonNegativeLong),
                instance.unassignedShards(),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 9 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                randomValueOtherThan(instance.unassignedShards(), ESTestCase::randomNonNegativeLong),
                instance.totalAllocations(),
                instance.undesiredAllocations()
            );
            case 10 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                randomValueOtherThan(instance.totalAllocations(), ESTestCase::randomNonNegativeLong),
                instance.undesiredAllocations()
            );
            case 11 -> new DesiredBalanceStats(
                instance.lastConvergedIndex(),
                instance.computationActive(),
                instance.computationSubmitted(),
                instance.computationExecuted(),
                instance.computationConverged(),
                instance.computationIterations(),
                instance.computedShardMovements(),
                instance.cumulativeComputationTime(),
                instance.cumulativeReconciliationTime(),
                instance.unassignedShards(),
                instance.totalAllocations(),
                randomValueOtherThan(instance.undesiredAllocations(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("failure, got illegal switch case");
        };
    }

    public void testToXContent() {
        var instance = createTestInstance();
        assertThat(
            Strings.toString(instance, true, false),
            equalTo(
                Strings.format(
                    """
                        {
                          "computation_converged_index" : %d,
                          "computation_active" : %b,
                          "computation_submitted" : %d,
                          "computation_executed" : %d,
                          "computation_converged" : %d,
                          "computation_iterations" : %d,
                          "computed_shard_movements" : %d,
                          "computation_time_in_millis" : %d,
                          "reconciliation_time_in_millis" : %d,
                          "unassigned_shards" : %d,
                          "total_allocations" : %d,
                          "undesired_allocations" : %d,
                          "undesired_allocations_ratio" : %s
                        }""",
                    instance.lastConvergedIndex(),
                    instance.computationActive(),
                    instance.computationSubmitted(),
                    instance.computationExecuted(),
                    instance.computationConverged(),
                    instance.computationIterations(),
                    instance.computedShardMovements(),
                    instance.cumulativeComputationTime(),
                    instance.cumulativeReconciliationTime(),
                    instance.unassignedShards(),
                    instance.totalAllocations(),
                    instance.undesiredAllocations(),
                    Double.toString(instance.undesiredAllocationsRatio())
                )
            )
        );
    }
}
