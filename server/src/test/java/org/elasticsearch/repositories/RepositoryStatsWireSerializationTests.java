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
        return new RepositoriesStats(generateRepositorySnapshotStatsMap());
    }

    private Map<String, RepositoriesStats.SnapshotStats> generateRepositorySnapshotStatsMap() {
        return randomMap(0, 5, () -> tuple(randomAlphaOfLength(10), randomSnapshotStats()));
    }

    @Override
    protected RepositoriesStats mutateInstance(RepositoriesStats instance) throws IOException {
        return new RepositoriesStats(randomValueOtherThan(instance.getRepositorySnapshotStats(), this::generateRepositorySnapshotStatsMap));
    }
}
