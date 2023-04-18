/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;

import java.util.HashMap;
import java.util.Map;

public class PolicyStatsTests extends AbstractWireSerializingTestCase<PolicyStats> {

    @Override
    protected PolicyStats createTestInstance() {
        return randomPolicyStats();
    }

    static PolicyStats randomPolicyStats() {
        Map<String, PhaseStats> phaseStats = new HashMap<>();
        int size = randomIntBetween(0, 10);
        for (int i = 0; i < size; i++) {
            phaseStats.put(randomAlphaOfLength(10), PhaseStatsTests.randomPhaseStats());
        }
        int numberIndicesManaged = randomIntBetween(0, 1000);
        return new PolicyStats(phaseStats, numberIndicesManaged);
    }

    @Override
    protected PolicyStats mutateInstance(PolicyStats instance) {
        Map<String, PhaseStats> phaseStats = instance.getPhaseStats();
        int numberIndicesManaged = instance.getIndicesManaged();
        switch (between(0, 1)) {
            case 0 -> {
                phaseStats = new HashMap<>(phaseStats);
                phaseStats.put(randomAlphaOfLength(11), PhaseStatsTests.randomPhaseStats());
            }
            case 1 -> numberIndicesManaged += randomIntBetween(1, 10);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PolicyStats(phaseStats, numberIndicesManaged);
    }

    @Override
    protected Reader<PolicyStats> instanceReader() {
        return PolicyStats::new;
    }

}
