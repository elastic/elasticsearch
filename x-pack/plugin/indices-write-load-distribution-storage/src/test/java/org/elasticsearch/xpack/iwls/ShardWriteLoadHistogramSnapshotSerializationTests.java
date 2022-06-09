/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.iwls;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ShardWriteLoadHistogramSnapshotSerializationTests extends AbstractSerializingTestCase<ShardWriteLoadHistogramSnapshot> {

    @Override
    protected ShardWriteLoadHistogramSnapshot doParseInstance(XContentParser parser) throws IOException {
        return ShardWriteLoadHistogramSnapshot.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ShardWriteLoadHistogramSnapshot> instanceReader() {
        return ShardWriteLoadHistogramSnapshot::new;
    }

    @Override
    protected ShardWriteLoadHistogramSnapshot createTestInstance() {
        return new ShardWriteLoadHistogramSnapshot(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)),
            randomBoolean(),
            randomLoadDistribution(),
            randomLoadDistribution(),
            randomLoadDistribution()
        );
    }

    @Override
    protected ShardWriteLoadHistogramSnapshot mutateInstance(ShardWriteLoadHistogramSnapshot instance) throws IOException {
        return new ShardWriteLoadHistogramSnapshot(
            instance.timestamp(),
            instance.dataStream(),
            new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)),
            randomBoolean(),
            randomLoadDistribution(),
            randomLoadDistribution(),
            randomLoadDistribution()
        );
    }

    private HistogramSnapshot randomLoadDistribution() {
        return new HistogramSnapshot(
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true)
        );
    }
}
