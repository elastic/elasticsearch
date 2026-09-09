/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ShardAndIndexHeapUsageTests extends ESTestCase {

    public void testCurrentWirePreservesSeparatedPostingsHeapUsage() throws IOException {
        final var heapUsage = new ShardAndIndexHeapUsage(10L, 20L, 30L);
        final ShardAndIndexHeapUsage readHeapUsage = copy(heapUsage, TransportVersion.current());
        assertThat(readHeapUsage, equalTo(heapUsage));
    }

    public void testLegacyWireFoldsPostingsIntoShardHeapUsage() throws IOException {
        final var legacyVersion = TransportVersionUtils.getPreviousVersion(ShardAndIndexHeapUsage.EXPLICIT_HEAP_ESTIMATE_COMPONENTS);
        final var heapUsage = new ShardAndIndexHeapUsage(10L, 20L, 30L);
        final ShardAndIndexHeapUsage readHeapUsage = copy(heapUsage, legacyVersion);
        assertThat(readHeapUsage.shardHeapUsageBytes(), equalTo(40L));
        assertThat(readHeapUsage.indexHeapUsageBytes(), equalTo(20L));
        assertThat(readHeapUsage.shardPostingsHeapUsageBytes(), equalTo(0L));
    }

    private static ShardAndIndexHeapUsage copy(ShardAndIndexHeapUsage heapUsage, TransportVersion transportVersion) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(transportVersion);
            heapUsage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(transportVersion);
                return new ShardAndIndexHeapUsage(in);
            }
        }
    }
}
