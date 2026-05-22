/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.OpenPITContextInfo;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.OpenPITReshardingState;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;

public class OpenPITContextInfoSerializationTests extends AbstractWireSerializingTestCase<OpenPITContextInfo> {

    @Override
    protected Writeable.Reader<OpenPITContextInfo> instanceReader() {
        return OpenPITContextInfo::new;
    }

    @Override
    protected OpenPITContextInfo createTestInstance() {
        return new OpenPITContextInfo(
            randomShardId(),
            randomAlphaOfLength(10),
            randomLongBetween(0, 1000),
            createSearchContextId(),
            createRandomMetadata(),
            randomReshardingState()
        );
    }

    SearchContextIdForNode createSearchContextId() {
        return new SearchContextIdForNode(
            randomAlphanumericOfLength(8),
            randomAlphanumericOfLength(8),
            new ShardSearchContextId(randomAlphaOfLength(10), randomLongBetween(0, 1000), randomAlphaOfLength(10))
        );
    }

    Map<String, BlobLocation> createRandomMetadata() {
        return randomMap(
            1,
            10,
            () -> new Tuple<>(
                randomAlphaOfLength(10),
                createBlobLocation(
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000),
                    randomLongBetween(1, 10000)
                )
            )
        );
    }

    OpenPITReshardingState randomReshardingState() {
        int shards = randomIntBetween(1, 10);
        if (randomBoolean()) {
            return new OpenPITReshardingState(null, SplitShardCountSummary.fromInt(shards));
        }
        return new OpenPITReshardingState(
            IndexReshardingMetadata.newSplitByMultiple(randomIntBetween(1, 10), 2),
            randomBoolean() ? SplitShardCountSummary.fromInt(shards) : SplitShardCountSummary.fromInt(shards * 2)
        );
    }

    @Override
    protected OpenPITContextInfo mutateInstance(OpenPITContextInfo instance) throws IOException {
        int i = randomIntBetween(0, 5);
        return switch (i) {
            case 0 -> new OpenPITContextInfo(
                randomValueOtherThan(instance.shardId(), OpenPITContextInfoSerializationTests::randomShardId),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata(),
                instance.reshardingState()
            );
            case 1 -> new OpenPITContextInfo(
                instance.shardId(),
                randomValueOtherThan(instance.segmentsFileName(), () -> randomAlphaOfLength(10)),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata(),
                instance.reshardingState()

            );
            case 2 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                randomValueOtherThan(instance.keepAlive(), () -> randomLongBetween(0, 1000)),
                instance.contextId(),
                instance.metadata(),
                instance.reshardingState()
            );
            case 3 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                randomValueOtherThan(instance.contextId(), this::createSearchContextId),
                instance.metadata(),
                instance.reshardingState()
            );
            case 4 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                createRandomMetadata(),
                instance.reshardingState()
            );
            case 5 -> new OpenPITContextInfo(
                instance.shardId(),
                instance.segmentsFileName(),
                instance.keepAlive(),
                instance.contextId(),
                instance.metadata(),
                randomValueOtherThan(instance.reshardingState(), this::randomReshardingState)
            );
            default -> throw new IllegalStateException("Unexpected value " + i);
        };
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
