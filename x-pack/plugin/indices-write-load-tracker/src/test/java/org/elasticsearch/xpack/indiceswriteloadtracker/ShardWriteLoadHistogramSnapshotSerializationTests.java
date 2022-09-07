/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xpack.indiceswriteloadtracker.HistogramSnapshotSerializationTests.mutateHistogramSnapshot;
import static org.elasticsearch.xpack.indiceswriteloadtracker.HistogramSnapshotSerializationTests.randomHistogramSnapshot;

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
            randomAlphaOfLength(10),
            new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)),
            randomBoolean(),
            new WriteLoadHistogramSnapshot(
                randomNonNegativeLong(),
                randomHistogramSnapshot(),
                randomHistogramSnapshot(),
                randomHistogramSnapshot()
            )
        );
    }

    @Override
    protected ShardWriteLoadHistogramSnapshot mutateInstance(ShardWriteLoadHistogramSnapshot instance) {
        final var mutationBranch = randomInt(6);
        return switch (mutationBranch) {
            case 0 -> new ShardWriteLoadHistogramSnapshot(
                randomAlphaOfLength(10),
                instance.shardId(),
                instance.primary(),
                instance.writeLoadHistogramSnapshot()
            );
            case 1 -> new ShardWriteLoadHistogramSnapshot(
                instance.dataStream(),
                new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)),
                instance.primary(),
                instance.writeLoadHistogramSnapshot()
            );
            case 2 -> new ShardWriteLoadHistogramSnapshot(
                instance.dataStream(),
                instance.shardId(),
                instance.primary() == false,
                instance.writeLoadHistogramSnapshot()
            );
            case 3 -> new ShardWriteLoadHistogramSnapshot(
                instance.dataStream(),
                instance.shardId(),
                instance.primary(),
                new WriteLoadHistogramSnapshot(
                    randomNonNegativeLong(),
                    instance.indexLoadHistogramSnapshot(),
                    instance.mergeLoadHistogramSnapshot(),
                    instance.refreshLoadHistogramSnapshot()
                )
            );
            case 4 -> new ShardWriteLoadHistogramSnapshot(
                instance.dataStream(),
                instance.shardId(),
                instance.primary(),
                new WriteLoadHistogramSnapshot(
                    instance.timestamp(),
                    mutateHistogramSnapshot(instance.indexLoadHistogramSnapshot()),
                    instance.mergeLoadHistogramSnapshot(),
                    instance.refreshLoadHistogramSnapshot()
                )
            );
            case 5 -> new ShardWriteLoadHistogramSnapshot(
                instance.dataStream(),
                instance.shardId(),
                instance.primary(),
                new WriteLoadHistogramSnapshot(
                    instance.timestamp(),
                    instance.indexLoadHistogramSnapshot(),
                    mutateHistogramSnapshot(instance.mergeLoadHistogramSnapshot()),
                    instance.refreshLoadHistogramSnapshot()
                )
            );
            case 6 -> new ShardWriteLoadHistogramSnapshot(
                instance.dataStream(),
                instance.shardId(),
                instance.primary(),
                new WriteLoadHistogramSnapshot(
                    instance.timestamp(),
                    instance.indexLoadHistogramSnapshot(),
                    instance.mergeLoadHistogramSnapshot(),
                    mutateHistogramSnapshot(instance.refreshLoadHistogramSnapshot())
                )
            );
            default -> throw new IllegalStateException("Unexpected value: " + mutationBranch);
        };
    }
}
