/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.repositories.SnapshotStatsWireSerializationTests.randomSnapshotStats;

public class RepositoryStatsWireSerializationTests extends AbstractWireSerializingTestCase<RepositoriesStats> {
    @Override
    protected Writeable.Reader<RepositoriesStats> instanceReader() {
        return RepositoriesStats::new;
    }

    @Override
    protected RepositoriesStats createTestInstance() {
        return new RepositoriesStats(randomMap(1, 5, () -> tuple(randomAlphaOfLength(10), randomSnapshotStats())));
    }

    @Override
    protected RepositoriesStats mutateInstance(RepositoriesStats instance) throws IOException {
        var original = instance.getRepositorySnapshotStats();
        var mutated = new HashMap<>(original);
        int mutation = randomIntBetween(0, 3);
        switch (mutation) {
            case 0 -> {
                // Replace an existing value
                String key = randomFrom(mutated.keySet());
                mutated.put(key, randomSnapshotStats());
            }
            case 1 -> {
                // Add a new entry
                mutated.put(randomAlphaOfLength(10), randomSnapshotStats());
            }
            case 2 -> {
                // Remove an entry, possibly resulting in an empty map
                mutated.remove(randomFrom(mutated.keySet()));
            }
            case 3 -> {
                // Change everything, rebuilding the map with different size
                Map<String, RepositoriesStats.SnapshotStats> rebuilt;
                do {
                    rebuilt = randomMap(1, 5, () -> tuple(randomAlphaOfLength(10), randomSnapshotStats()));
                } while (rebuilt.equals(original));
                mutated.clear();
                mutated.putAll(rebuilt);
            }
        }
        return new RepositoriesStats(mutated);
    }
}
