/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SnapshotLifecycleStatsTests extends AbstractSerializingTestCase<SnapshotLifecycleStats> {
    @Override
    protected SnapshotLifecycleStats doParseInstance(XContentParser parser) throws IOException {
        return SnapshotLifecycleStats.parse(parser);
    }

    public static SnapshotLifecycleStats.SnapshotPolicyStats randomPolicyStats(String policyId) {
        return new SnapshotLifecycleStats.SnapshotPolicyStats(policyId,
            randomBoolean() ? 0 : randomNonNegativeLong(),
            randomBoolean() ? 0 : randomNonNegativeLong(),
            randomBoolean() ? 0 : randomNonNegativeLong(),
            randomBoolean() ? 0 : randomNonNegativeLong());
    }

    public static SnapshotLifecycleStats randomLifecycleStats() {
        int policies = randomIntBetween(0, 5);
        Map<String, SnapshotLifecycleStats.SnapshotPolicyStats> policyStats = new HashMap<>(policies);
        for (int i = 0; i < policies; i++) {
            String policy = "policy-" + randomAlphaOfLength(4);
            policyStats.put(policy, randomPolicyStats(policy));
        }
        return new SnapshotLifecycleStats(
            randomBoolean() ? 0 : randomNonNegativeLong(),
            randomBoolean() ? 0 : randomNonNegativeLong(),
            randomBoolean() ? 0 : randomNonNegativeLong(),
            randomBoolean() ? 0 : randomNonNegativeLong(),
            policyStats);
    }

    @Override
    protected SnapshotLifecycleStats createTestInstance() {
        return randomLifecycleStats();
    }

    @Override
    protected SnapshotLifecycleStats mutateInstance(SnapshotLifecycleStats instance) throws IOException {
        return randomValueOtherThan(instance, () -> instance.merge(createTestInstance()));
    }

    @Override
    protected Writeable.Reader<SnapshotLifecycleStats> instanceReader() {
        return SnapshotLifecycleStats::new;
    }
}
