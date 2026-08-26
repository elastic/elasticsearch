/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

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

    /**
     * Verifies wire compatibility with transport versions older than
     * {@link RepositoriesStats.SnapshotStats#EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO}.
     *
     * <p>For these older versions, only the throttling-related fields are serialized.
     * All extended snapshot statistics introduced later must be dropped during
     * serialization and restored to their default values when deserialized.</p>
     *
     * <p>This ensures nodes running newer versions can safely communicate snapshot
     * statistics to older nodes without breaking wire compatibility.</p>
     */
    public void testSerializationWithTransportVersionBeforeExtendedSnapshotStatsInNodeInfo() throws IOException {
        final TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(
            RepositoriesStats.SnapshotStats.EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO
        );
        final RepositoriesStats.SnapshotStats instance = randomSnapshotStats();
        final RepositoriesStats.SnapshotStats deserialized = copyInstance(instance, oldVersion);

        // Older versions only carry throttling stats, so we expect all other values to be 0
        assertThat(deserialized.shardSnapshotsStarted(), equalTo(0L));
        assertThat(deserialized.shardSnapshotsCompleted(), equalTo(0L));
        assertThat(deserialized.shardSnapshotsInProgress(), equalTo(0L));
        assertThat(deserialized.totalReadThrottledNanos(), equalTo(instance.totalReadThrottledNanos()));
        assertThat(deserialized.totalWriteThrottledNanos(), equalTo(instance.totalWriteThrottledNanos()));
        assertThat(deserialized.numberOfBlobsUploaded(), equalTo(0L));
        assertThat(deserialized.numberOfBytesUploaded(), equalTo(0L));
        assertThat(deserialized.totalUploadTimeInMillis(), equalTo(0L));
        assertThat(deserialized.totalUploadReadTimeInMillis(), equalTo(0L));
    }

    public static RepositoriesStats.SnapshotStats randomSnapshotStats() {
        return randomBoolean()
            ? RepositoriesStats.SnapshotStats.ZERO
            : new RepositoriesStats.SnapshotStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
    }
}
