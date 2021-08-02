/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class GetShardSnapshotRequestSerializationTests extends AbstractWireSerializingTestCase<GetShardSnapshotRequest> {
    @Override
    protected Writeable.Reader<GetShardSnapshotRequest> instanceReader() {
        return GetShardSnapshotRequest::new;
    }

    @Override
    protected GetShardSnapshotRequest createTestInstance() {
        ShardId shardId = randomShardId();
        if (randomBoolean()) {
            return GetShardSnapshotRequest.latestSnapshotInAllRepositories(shardId);
        } else {
            List<String> repositories = randomList(1, randomIntBetween(1, 100), () -> randomAlphaOfLength(randomIntBetween(1, 100)));
            return GetShardSnapshotRequest.latestSnapshotInRepositories(shardId, repositories);
        }
    }

    @Override
    protected GetShardSnapshotRequest mutateInstance(GetShardSnapshotRequest instance) throws IOException {
        ShardId shardId = randomShardId();
        if (instance.getFromAllRepositories()) {
            return GetShardSnapshotRequest.latestSnapshotInAllRepositories(shardId);
        } else {
            return GetShardSnapshotRequest.latestSnapshotInRepositories(shardId, instance.getRepositories());
        }
    }

    private ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 100));
    }
}
