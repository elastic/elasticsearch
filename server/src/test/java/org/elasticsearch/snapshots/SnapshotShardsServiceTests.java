/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class SnapshotShardsServiceTests extends ESTestCase {

    public void testEqualsAndHashcodeUpdateIndexShardSnapshotStatusRequest() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new UpdateIndexShardSnapshotStatusRequest(
                new Snapshot(randomAlphaOfLength(10),
                    new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))),
                new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()), randomInt(5)),
                new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))),
            request ->
                new UpdateIndexShardSnapshotStatusRequest(request.snapshot(), request.shardId(), request.status()),
            request -> {
                final boolean mutateSnapshot = randomBoolean();
                final boolean mutateShardId = randomBoolean();
                final boolean mutateStatus = (mutateSnapshot || mutateShardId) == false || randomBoolean();
                return new UpdateIndexShardSnapshotStatusRequest(
                    mutateSnapshot ? new Snapshot(randomAlphaOfLength(10),
                        new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))) : request.snapshot(),
                    mutateShardId ?
                        new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()), randomInt(5)) : request.shardId(),
                    mutateStatus ? new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))
                        : request.status()
                );
            });
    }

}
