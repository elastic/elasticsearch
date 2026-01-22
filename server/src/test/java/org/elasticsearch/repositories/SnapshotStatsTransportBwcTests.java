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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests whether the {@link RepositoriesStats.SnapshotStats} object can be correctly serialised and deserialised
 * when sent between two nodes on different transport versions
 */
public class SnapshotStatsTransportBwcTests extends ESTestCase {

    /**
     * {@link RepositoriesStats.SnapshotStats#UPLOAD_TIME_NANOS} converted the {@code totalUploadTimeInNanos} and
     * {@code totalUploadReadTimeInNanos} fields from milliseconds to nanoseconds. This tests when a node is
     * communicating with a node on a previous version and we expect these fields to be converted into milliseconds.
     * @throws IOException
     */
    public void testWritingToOldTransportVersionConvertsNanosToMillis() throws IOException {
        long totalUploadTimeInNanos = randomNonNegativeLong();
        long totalUploadReadTimeInNanos = randomNonNegativeLong();
        RepositoriesStats.SnapshotStats stats = new RepositoriesStats.SnapshotStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            totalUploadTimeInNanos,
            totalUploadReadTimeInNanos
        );
        TransportVersion transportVersionWithMillisecondUploadTime = TransportVersionUtils.getPreviousVersion(
            RepositoriesStats.SnapshotStats.UPLOAD_TIME_NANOS
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(transportVersionWithMillisecondUploadTime);
        stats.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(transportVersionWithMillisecondUploadTime);
        RepositoriesStats.SnapshotStats read = RepositoriesStats.SnapshotStats.readFrom(in);

        // We expect the nanoseconds to be converted to milliseconds when writing to the old node
        long expectedTotalUploadTimeInNanos = TimeUnit.NANOSECONDS.toMillis(totalUploadTimeInNanos);
        long expectedTotalUploadReadTimeInNanos = TimeUnit.NANOSECONDS.toMillis(totalUploadReadTimeInNanos);

        // We expect the milliseconds to be converted to nanoseconds when reading from the old node
        expectedTotalUploadTimeInNanos = TimeUnit.MILLISECONDS.toNanos(expectedTotalUploadTimeInNanos);
        expectedTotalUploadReadTimeInNanos = TimeUnit.MILLISECONDS.toNanos(expectedTotalUploadReadTimeInNanos);

        assertEquals(expectedTotalUploadTimeInNanos, read.totalUploadTimeInNanos());
        assertEquals(expectedTotalUploadReadTimeInNanos, read.totalUploadReadTimeInNanos());
    }

    /**
     * {@link RepositoriesStats.SnapshotStats#UPLOAD_TIME_NANOS} converted the {@code totalUploadTimeInNanos} and
     * {@code totalUploadReadTimeInNanos} fields from milliseconds to nanoseconds. This tests when a node is
     * reading from a node on a previous transport version in which we expect the milliseconds to be converted
     * into nanoseconds
     * @throws IOException
     */
    public void testReadingFromOldTransportVersionConvertsMillisToNanos() throws IOException {
        long totalUploadTimeInNanos = randomNonNegativeLong();
        long totalUploadReadTimeInNanos = randomNonNegativeLong();
        TransportVersion transportVersionWithMillisecondUploadTime = TransportVersionUtils.getPreviousVersion(
            RepositoriesStats.SnapshotStats.UPLOAD_TIME_NANOS
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(transportVersionWithMillisecondUploadTime);

        // Randomly simulate a throttled snapshot
        if (randomBoolean()) {
            out.writeVLong(0); // read throttled
            out.writeVLong(0); // write throttled
            out.writeVLong(0);
            out.writeVLong(0);
            out.writeVLong(0);
            out.writeVLong(0);
            out.writeVLong(0);
            out.writeVLong(totalUploadTimeInNanos);
            out.writeVLong(totalUploadReadTimeInNanos);
        } else {
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(randomNonNegativeLong());
            out.writeVLong(totalUploadTimeInNanos);
            out.writeVLong(totalUploadReadTimeInNanos);
        }

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(transportVersionWithMillisecondUploadTime);
        RepositoriesStats.SnapshotStats stats = RepositoriesStats.SnapshotStats.readFrom(in);

        // Expect the node on the newer transport version to convert the upload time fields from
        // milliseconds to nanoseconds
        assertEquals(TimeUnit.MILLISECONDS.toNanos(totalUploadTimeInNanos), stats.totalUploadTimeInNanos());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(totalUploadReadTimeInNanos), stats.totalUploadReadTimeInNanos());
    }
}
