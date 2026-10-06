/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class ShardHeapUsageEstimatesTests extends ESTestCase {

    public void testGetOrDefault() {
        final var defaultUsage = randomShardAndIndexHeapUsage();
        final var shardId = randomShardId();
        final var otherShardId = randomValueOtherThan(shardId, ShardHeapUsageEstimatesTests::randomShardId);
        final var estimates = new ShardHeapUsageEstimates(Map.of(otherShardId, randomShardAndIndexHeapUsage()), defaultUsage);

        assertEquals(defaultUsage, estimates.getOrDefault(shardId));
    }

    private static ShardId randomShardId() {
        return new ShardId(new Index(randomIndexName(), randomUUID()), randomNonNegativeInt());
    }

    private static ShardAndIndexHeapUsage randomShardAndIndexHeapUsage() {
        final long shardHeapUsageBytes = randomNonNegativeLong();
        final long postingsHeapUsageBytes = randomLongBetween(0, shardHeapUsageBytes);
        return new ShardAndIndexHeapUsage(shardHeapUsageBytes, randomNonNegativeLong(), postingsHeapUsageBytes);
    }
}
