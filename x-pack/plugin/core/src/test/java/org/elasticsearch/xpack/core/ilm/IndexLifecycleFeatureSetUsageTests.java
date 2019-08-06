/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.PolicyStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexLifecycleFeatureSetUsageTests extends AbstractWireSerializingTestCase<IndexLifecycleFeatureSetUsage> {

    @Override
    protected IndexLifecycleFeatureSetUsage createTestInstance() {
        boolean enabled = randomBoolean();
        boolean available = randomBoolean();
        List<PolicyStats> policyStats = null;
        if (enabled) {
            int size = randomIntBetween(0, 10);
            policyStats = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                policyStats.add(PolicyStatsTests.createRandomInstance());
            }
        }
        return new IndexLifecycleFeatureSetUsage(available, enabled, policyStats);
    }

    @Override
    protected IndexLifecycleFeatureSetUsage mutateInstance(IndexLifecycleFeatureSetUsage instance) throws IOException {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        List<PolicyStats> policyStats = instance.getPolicyStats();
        switch (between(0, 2)) {
        case 0:
            available = available == false;
            break;
        case 1:
            enabled = enabled == false;
            break;
        case 2:
            if (policyStats == null) {
                policyStats = new ArrayList<>();
                policyStats.add(PolicyStatsTests.createRandomInstance());
            } else if (randomBoolean()) {
                policyStats = null;
            } else {
                policyStats = new ArrayList<>(policyStats);
                policyStats.add(PolicyStatsTests.createRandomInstance());
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new IndexLifecycleFeatureSetUsage(available, enabled, policyStats);
    }

    @Override
    protected Reader<IndexLifecycleFeatureSetUsage> instanceReader() {
        return IndexLifecycleFeatureSetUsage::new;
    }

}
