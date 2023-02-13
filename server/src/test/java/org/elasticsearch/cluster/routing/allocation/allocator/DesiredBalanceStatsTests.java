/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceStatsTests extends AbstractWireSerializingTestCase<DesiredBalanceStats> {

    @Override
    protected Writeable.Reader<DesiredBalanceStats> instanceReader() {
        return DesiredBalanceStats::readFrom;
    }

    @Override
    protected DesiredBalanceStats createTestInstance() {
        return new DesiredBalanceStats(
            randomNonNegativeLong(),
            randomBoolean(),
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
        return createTestInstance();
    }

    public void testToXContent() {
        var instance = createTestInstance();
        assertThat(
            Strings.toString(instance, true, false),
            equalTo(
                String.format(
                    Locale.ROOT,
                    """
                        {
                          "computation_active" : %b,
                          "computation_submitted" : %d,
                          "computation_executed" : %d,
                          "computation_converged" : %d,
                          "computation_iterations" : %d,
                          "computation_converged_index" : %d,
                          "computation_time_in_millis" : %d,
                          "reconciliation_time_in_millis" : %d
                        }""",
                    instance.computationActive(),
                    instance.computationSubmitted(),
                    instance.computationExecuted(),
                    instance.computationConverged(),
                    instance.computationIterations(),
                    instance.lastConvergedIndex(),
                    instance.cumulativeComputationTime(),
                    instance.cumulativeReconciliationTime()
                )
            )
        );
    }
}
