/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SnapshotLifecycleStatsTests extends AbstractXContentSerializingTestCase<SnapshotLifecycleStats> {

    public void testPolicyStatsMapImmutable() throws IOException {
        {
            SnapshotLifecycleStats stats = new SnapshotLifecycleStats();
            assertThrows(UnsupportedOperationException.class, () -> stats.getMetrics().put("new_policy", null));
        }

        {
            SnapshotLifecycleStats stats = new SnapshotLifecycleStats(0, 0, 0, 0, new HashMap<>());
            assertThrows(UnsupportedOperationException.class, () -> stats.getMetrics().put("new_policy", null));
        }

        {
            SnapshotLifecycleStats stats1 = new SnapshotLifecycleStats(1, 0, 0, 0, new HashMap<>());
            SnapshotLifecycleStats stats2 = new SnapshotLifecycleStats(0, 1, 0, 0, new HashMap<>());
            SnapshotLifecycleStats stats = stats1.merge(stats2);
            assertThrows(UnsupportedOperationException.class, () -> stats.getMetrics().put("new_policy", null));
        }

        {
            // write
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            SnapshotLifecycleStats stats1 = new SnapshotLifecycleStats(0, 0, 0, 0, new HashMap<>());
            stats1.writeTo(out);

            // read
            ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
            SnapshotLifecycleStats stats = new SnapshotLifecycleStats(in);
            assertThrows(UnsupportedOperationException.class, () -> stats.getMetrics().put("new_policy", null));
        }
    }

    public void testIncrementAllFields() {
        final String policy = "policy";
        SnapshotLifecycleStats stats = new SnapshotLifecycleStats();
        SnapshotLifecycleStats updated = stats.withRetentionRunIncremented()
            .withRetentionFailedIncremented()
            .withRetentionTimedOutIncremented()
            .withDeletionTimeUpdated(TimeValue.ONE_MINUTE)
            .withTakenIncremented(policy)
            .withDeletedIncremented(policy)
            .withFailedIncremented(policy)
            .withDeleteFailureIncremented(policy);

        var policyStats = Map.of(policy, new SnapshotLifecycleStats.SnapshotPolicyStats(policy, 1, 1, 1, 1));
        SnapshotLifecycleStats expected = new SnapshotLifecycleStats(1, 1, 1, TimeValue.ONE_MINUTE.millis(), policyStats);
        assertEquals(expected, updated);
    }

    @Override
    protected SnapshotLifecycleStats doParseInstance(XContentParser parser) throws IOException {
        return SnapshotLifecycleStats.parse(parser);
    }

    public static SnapshotLifecycleStats.SnapshotPolicyStats randomPolicyStats(String policyId) {
        return new SnapshotLifecycleStats.SnapshotPolicyStats(
            policyId,
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE)
        );
    }

    public static SnapshotLifecycleStats randomLifecycleStats() {
        int policies = randomIntBetween(0, 5);
        Map<String, SnapshotLifecycleStats.SnapshotPolicyStats> policyStats = Maps.newMapWithExpectedSize(policies);
        for (int i = 0; i < policies; i++) {
            String policy = "policy-" + randomAlphaOfLength(4);
            policyStats.put(policy, randomPolicyStats(policy));
        }
        return new SnapshotLifecycleStats(
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE),
            policyStats
        );
    }

    @Override
    protected SnapshotLifecycleStats createTestInstance() {
        return randomLifecycleStats();
    }

    @Override
    protected SnapshotLifecycleStats mutateInstance(SnapshotLifecycleStats instance) {
        List<String> policies = new ArrayList<>(instance.getMetrics().keySet());
        String policy = policies.isEmpty() ? "policy-" + randomAlphaOfLength(4) : randomFrom(policies);

        return switch (between(0, 7)) {
            case 0 -> instance.withRetentionRunIncremented();
            case 1 -> instance.withRetentionFailedIncremented();
            case 2 -> instance.withRetentionTimedOutIncremented();
            case 3 -> instance.withDeletionTimeUpdated(randomTimeValue(1, 1_000_000, TimeUnit.MILLISECONDS));
            case 4 -> instance.withTakenIncremented(policy);
            case 5 -> instance.withFailedIncremented(policy);
            case 6 -> instance.withDeletedIncremented(policy);
            default -> instance.withDeleteFailureIncremented(policy);
        };
    }

    @Override
    protected Writeable.Reader<SnapshotLifecycleStats> instanceReader() {
        return SnapshotLifecycleStats::new;
    }
}
