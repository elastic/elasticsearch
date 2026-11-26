/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class IndexDiskUsageStatsTests extends ESTestCase {

    public void testEmptySerialization() throws IOException {
        IndexDiskUsageStats emptyDndexDiskUsageStats = createEmptyDiskUsageStats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            emptyDndexDiskUsageStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                IndexDiskUsageStats deserializedNodeStats = new IndexDiskUsageStats(in);
                assertEquals(emptyDndexDiskUsageStats.getIndexSizeInBytes(), deserializedNodeStats.getIndexSizeInBytes());
                assertEquals(emptyDndexDiskUsageStats.getFields(), deserializedNodeStats.getFields());
                // Now just making sure that an exception doesn't get thrown here:
                deserializedNodeStats.addStoredField(randomAlphaOfLength(10), randomNonNegativeLong());
            }
        }
    }

    public void testSkipperBytesSerialization() throws IOException {
        final TransportVersion skipperVersion = TransportVersion.fromName("disk_usage_stats_doc_values_skipper");
        final IndexDiskUsageStats stats = new IndexDiskUsageStats(randomNonNegativeLong());
        stats.addDocValues("field", 100);
        stats.addDocValuesSkipper("field", 40);
        stats.addInvertedIndex("field", 7);

        final IndexDiskUsageStats.PerFieldDiskUsage current = roundTrip(stats, skipperVersion).getFields().get("field");
        assertEquals(100, current.getDocValuesBytes());
        assertEquals(40, current.getDocValuesSkipperBytes());
        assertEquals(stats.total().totalBytes(), current.totalBytes());

        // older nodes have no skipper slot, so skipper bytes are folded into doc values and the total is unchanged
        final TransportVersion legacyVersion = TransportVersionUtils.getPreviousVersion(skipperVersion);
        final IndexDiskUsageStats.PerFieldDiskUsage legacy = roundTrip(stats, legacyVersion).getFields().get("field");
        assertEquals(140, legacy.getDocValuesBytes());
        assertEquals(0, legacy.getDocValuesSkipperBytes());
        assertEquals(stats.total().totalBytes(), legacy.totalBytes());
    }

    private static IndexDiskUsageStats roundTrip(IndexDiskUsageStats stats, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new IndexDiskUsageStats(in);
            }
        }
    }

    private IndexDiskUsageStats createEmptyDiskUsageStats() {
        return new IndexDiskUsageStats(randomNonNegativeLong());
    }

}
