/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;

import java.util.ArrayList;
import java.util.List;

public class IndexLifecycleFeatureSetUsageTests extends AbstractWireSerializingTestCase<IndexLifecycleFeatureSetUsage> {

    @Override
    protected IndexLifecycleFeatureSetUsage createTestInstance() {
        List<PolicyStats> policyStats = new ArrayList<>();
        int size = randomIntBetween(0, 10);
        for (int i = 0; i < size; i++) {
            policyStats.add(PolicyStatsTests.randomPolicyStats());
        }
        return new IndexLifecycleFeatureSetUsage(policyStats);
    }

    @Override
    protected IndexLifecycleFeatureSetUsage mutateInstance(IndexLifecycleFeatureSetUsage instance) {
        boolean available = instance.available();
        List<PolicyStats> policyStats = instance.getPolicyStats();
        policyStats = new ArrayList<>(policyStats);
        policyStats.add(PolicyStatsTests.randomPolicyStats());
        return new IndexLifecycleFeatureSetUsage(policyStats);
    }

    @Override
    protected Reader<IndexLifecycleFeatureSetUsage> instanceReader() {
        return IndexLifecycleFeatureSetUsage::new;
    }

}
