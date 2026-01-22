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

import static org.elasticsearch.repositories.SnapshotStatsTestUtils.randomSnapshotStats;

public class SnapshotStatsWireSerializationTests extends AbstractWireSerializingTestCase<RepositoriesStats.SnapshotStats> {
    @Override
    protected Writeable.Reader<RepositoriesStats.SnapshotStats> instanceReader() {
        return RepositoriesStats.SnapshotStats::readFrom;
    }

    @Override
    protected RepositoriesStats.SnapshotStats createTestInstance() {
        return randomSnapshotStats();
    }

    @Override
    protected RepositoriesStats.SnapshotStats mutateInstance(RepositoriesStats.SnapshotStats instance) throws IOException {
        // Since SnapshotStats is a record, we don't need to check for equality
        return null;
    }
}
