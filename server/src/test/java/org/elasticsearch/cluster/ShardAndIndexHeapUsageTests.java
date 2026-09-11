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

import static org.elasticsearch.cluster.NodeHeapEstimates.EXPLICIT_HEAP_ESTIMATE_COMPONENTS;
import static org.hamcrest.Matchers.equalTo;

public class ShardAndIndexHeapUsageTests extends ESTestCase {

    public void testShardHeapUsageIncludingPostingsBytes() {
        // This helper returns shard-local heap only, so the 20 byte index overhead is excluded: 10 shard heap + 30 postings = 40.
        assertThat(new ShardAndIndexHeapUsage(10L, 20L, 30L).shardHeapUsageIncludingPostingsBytes(), equalTo(40L));
    }

    public void testSerializationScenarios() throws IOException {
        final var legacyVersion = TransportVersionUtils.getPreviousVersion(EXPLICIT_HEAP_ESTIMATE_COMPONENTS);
        final var newData = new ShardAndIndexHeapUsage(10L, 20L, 30L);
        final var oldData = new ShardAndIndexHeapUsage(10L, 20L, 0L);

        // New code + new data: postings stays separated on the current wire format.
        assertThat(copy(newData, TransportVersion.current()), equalTo(newData));

        // Old code + new data: legacy readers only know shard/index heap, so postings is folded into shard heap.
        assertThat(copy(newData, legacyVersion), equalTo(new ShardAndIndexHeapUsage(40L, 20L, 0L)));

        // New code + old data: a zero-postings value remains explicit on the current wire format.
        assertThat(copy(oldData, TransportVersion.current()), equalTo(oldData));

        // Old code + old data: there is nothing to fold, so the legacy shape is unchanged.
        assertThat(copy(oldData, legacyVersion), equalTo(oldData));
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
