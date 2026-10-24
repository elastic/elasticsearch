/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.node.HealthInfoTests;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalRecoveryRetriesHealthInfoTests extends AbstractWireSerializingTestCase<LocalRecoveryRetriesHealthInfo> {

    @Override
    protected Writeable.Reader<LocalRecoveryRetriesHealthInfo> instanceReader() {
        return LocalRecoveryRetriesHealthInfo::new;
    }

    @Override
    protected LocalRecoveryRetriesHealthInfo createTestInstance() {
        return randomBoolean() ? LocalRecoveryRetriesHealthInfo.EMPTY : HealthInfoTests.randomLocalRecoveryRetriesHealthInfo();
    }

    @Override
    protected LocalRecoveryRetriesHealthInfo mutateInstance(LocalRecoveryRetriesHealthInfo instance) throws IOException {
        Map<ShardId, Integer> mutated = new HashMap<>(instance.localRetries());
        if (mutated.isEmpty() || randomBoolean()) {
            // Add a shard (the random shard ID will not collide with an existing one in practice, and we check below in case it does).
            ShardId newShardId = new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(22), randomIntBetween(0, 5));
            if (mutated.putIfAbsent(newShardId, randomIntBetween(1, 10)) != null) {
                return mutateInstance(instance);
            }
        } else {
            // Change the retry count for an existing shard.
            ShardId shardId = randomFrom(mutated.keySet());
            mutated.put(shardId, randomValueOtherThan(mutated.get(shardId), () -> randomIntBetween(1, 10)));
        }
        return new LocalRecoveryRetriesHealthInfo(mutated);
    }
}
