/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

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

    private IndexDiskUsageStats createEmptyDiskUsageStats() {
        return new IndexDiskUsageStats(randomNonNegativeLong());
    }

}
