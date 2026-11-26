/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.routing.allocation.shards.LocalRecoveryRetriesHealthInfo.RecoveryKey;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.node.HealthInfoTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

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
        Map<RecoveryKey, Integer> mutated = new HashMap<>(instance.localRetries());
        switch (mutated.isEmpty() ? 0 : randomIntBetween(0, 2)) {
            case 0 -> mutated.put(HealthInfoTests.randomRecoveryKey(), randomIntBetween(1, 10));
            case 1 -> {
                // Change the retry count for an existing recovery.
                RecoveryKey key = randomFrom(mutated.keySet());
                mutated.put(key, randomValueOtherThan(mutated.get(key), () -> randomIntBetween(1, 10)));
            }
            case 2 -> {
                // Change only the allocation ID of an existing recovery, keeping the shard ID and the count.
                RecoveryKey key = randomFrom(mutated.keySet());
                mutated.put(
                    new RecoveryKey(key.shardId(), randomValueOtherThan(key.allocationId(), ESTestCase::randomUUID)),
                    mutated.remove(key)
                );
            }
            default -> throw new AssertionError("unexpected case");
        }
        return new LocalRecoveryRetriesHealthInfo(mutated);
    }
}
