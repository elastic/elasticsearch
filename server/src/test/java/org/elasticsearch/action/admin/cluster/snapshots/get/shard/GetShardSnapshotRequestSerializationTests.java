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

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
    protected GetShardSnapshotRequest mutateInstance(GetShardSnapshotRequest instance) {
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

    public void testGetDescription() {
        final GetShardSnapshotRequest request = new GetShardSnapshotRequest(Arrays.asList("repo1", "repo2"), new ShardId("idx", "uuid", 0));
        assertThat(request.getDescription(), equalTo("shard[idx][0], repositories[repo1,repo2]"));

        final GetShardSnapshotRequest randomRequest = createTestInstance();
        final String description = randomRequest.getDescription();
        assertThat(description, containsString(randomRequest.getShardId().toString()));
        assertThat(
            description,
            description.length(),
            lessThanOrEqualTo(
                ("shard" + randomRequest.getShardId() + ", repositories[").length() + 1024 + 100 + ",... (999 in total, 999 omitted)"
                    .length()
            )
        );
    }
}
