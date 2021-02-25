/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PhaseStats;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PolicyStatsTests extends AbstractWireSerializingTestCase<PolicyStats> {

    @Override
    protected PolicyStats createTestInstance() {
        return createRandomInstance();
    }

    public static PolicyStats createRandomInstance() {
        int size = randomIntBetween(0, 10);
        Map<String, PhaseStats> phaseStats = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            phaseStats.put(randomAlphaOfLengthBetween(1, 20), PhaseStatsTests.createRandomInstance());
        }
        return new PolicyStats(phaseStats, randomIntBetween(0, 100));
    }

    @Override
    protected PolicyStats mutateInstance(PolicyStats instance) throws IOException {
        Map<String, PhaseStats> phaseStats = instance.getPhaseStats();
        int indicesManaged = instance.getIndicesManaged();
        switch (between(0, 1)) {
        case 0:
            phaseStats = new HashMap<>(instance.getPhaseStats());
            phaseStats.put(randomAlphaOfLengthBetween(21, 25), PhaseStatsTests.createRandomInstance());
            break;
        case 1:
            indicesManaged += randomIntBetween(1, 10);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new PolicyStats(phaseStats, indicesManaged);
    }

    @Override
    protected Reader<PolicyStats> instanceReader() {
        return PolicyStats::new;
    }

}
