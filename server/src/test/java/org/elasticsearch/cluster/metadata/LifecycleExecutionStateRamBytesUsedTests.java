/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class LifecycleExecutionStateRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return LifecycleExecutionState.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of(
            "phase",
            "action",
            "step",
            "failedStep",
            "isAutoRetryableError",
            "failedStepRetryCount",
            "stepInfo",
            "previousStepInfo",
            "phaseDefinition",
            "lifecycleDate",
            "phaseTime",
            "actionTime",
            "stepTime",
            "snapshotRepository",
            "snapshotName",
            "shrinkIndexName",
            "snapshotIndexName",
            "downsampleIndexName",
            "forceMergeCloneIndexName"
        );
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        return LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("check-rollover-ready")
            .setStepInfo("{\"some\":\"info\"}")
            .build();
    }

    /**
     * Non-tautology check: populating string fields must increase the estimate over the empty state.
     */
    public void testRamBytesUsedGrowsWhenPopulated() {
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(LifecycleExecutionState.EMPTY_STATE.ramBytesUsed()));
    }
}
