/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SnapshotShardFailureSerializationTests extends AbstractXContentTestCase<SnapshotShardFailure> {

    public void testEqualsAndHashCode() {
        final SnapshotShardFailure instance = createTestInstance();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            instance,
            snapshotShardFailure -> new SnapshotShardFailure(
                snapshotShardFailure.nodeId(),
                snapshotShardFailure.getShardId(),
                snapshotShardFailure.reason()
            ),
            snapshotShardFailure -> {
                if (randomBoolean()) {
                    return new SnapshotShardFailure(
                        randomValueOtherThan(snapshotShardFailure.nodeId(), () -> randomAlphaOfLengthBetween(5, 10)),
                        snapshotShardFailure.getShardId(),
                        snapshotShardFailure.reason()
                    );
                } else if (randomBoolean()) {
                    return new SnapshotShardFailure(
                        snapshotShardFailure.nodeId(),
                        snapshotShardFailure.getShardId(),
                        randomValueOtherThan(snapshotShardFailure.reason(), () -> randomAlphaOfLengthBetween(1, 100))
                    );
                } else {
                    final ShardId originalShardId = snapshotShardFailure.getShardId();
                    final ShardId mutatedShardId;
                    if (randomBoolean()) {
                        mutatedShardId = new ShardId(
                            originalShardId.getIndex(),
                            randomValueOtherThan((byte) originalShardId.getId(), ESTestCase::randomNonNegativeByte)
                        );
                    } else if (randomBoolean()) {
                        mutatedShardId = new ShardId(
                            new Index(
                                originalShardId.getIndexName(),
                                randomValueOtherThan(originalShardId.getIndex().getUUID(), () -> UUIDs.randomBase64UUID(random()))
                            ),
                            originalShardId.id()
                        );
                    } else {
                        mutatedShardId = randomValueOtherThan(originalShardId, SnapshotShardFailureSerializationTests::randomShardId);
                    }
                    return new SnapshotShardFailure(snapshotShardFailure.nodeId(), mutatedShardId, snapshotShardFailure.reason());
                }
            }
        );
    }

    @Override
    protected SnapshotShardFailure createTestInstance() {
        return new SnapshotShardFailure(UUIDs.randomBase64UUID(random()), randomShardId(), randomAlphaOfLengthBetween(1, 100));
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLengthBetween(5, 10), UUIDs.randomBase64UUID(random()), randomNonNegativeByte());
    }

    @Override
    protected SnapshotShardFailure doParseInstance(XContentParser parser) throws IOException {
        return SnapshotShardFailure.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
