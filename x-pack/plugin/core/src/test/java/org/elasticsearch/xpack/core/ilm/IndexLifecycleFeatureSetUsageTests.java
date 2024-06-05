/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;

import java.util.ArrayList;
import java.util.List;

public class IndexLifecycleFeatureSetUsageTests extends AbstractWireSerializingTestCase<IndexLifecycleFeatureSetUsage> {

    @Override
    protected IndexLifecycleFeatureSetUsage createTestInstance() {
        boolean enabled = randomBoolean();
        List<PolicyStats> policyStats = null;
        if (enabled) {
            int size = randomIntBetween(0, 10);
            policyStats = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                policyStats.add(PolicyStatsTests.createRandomInstance());
            }
        }
        return new IndexLifecycleFeatureSetUsage(policyStats);
    }

    @Override
    protected IndexLifecycleFeatureSetUsage mutateInstance(IndexLifecycleFeatureSetUsage instance) {
        List<PolicyStats> policyStats = instance.getPolicyStats();
        if (policyStats == null) {
            policyStats = new ArrayList<>();
            policyStats.add(PolicyStatsTests.createRandomInstance());
        } else if (randomBoolean()) {
            policyStats = null;
        } else {
            policyStats = new ArrayList<>(policyStats);
            policyStats.add(PolicyStatsTests.createRandomInstance());
        }
        return new IndexLifecycleFeatureSetUsage(policyStats);
    }

    @Override
    protected Reader<IndexLifecycleFeatureSetUsage> instanceReader() {
        return IndexLifecycleFeatureSetUsage::new;
    }

}
