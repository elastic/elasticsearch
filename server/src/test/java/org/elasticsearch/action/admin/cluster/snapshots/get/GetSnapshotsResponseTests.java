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

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetSnapshotsResponseTests extends AbstractStreamableXContentTestCase<GetSnapshotsResponse> {

    @Override
    protected GetSnapshotsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSnapshotsResponse.fromXContent(parser);
    }

    @Override
    protected GetSnapshotsResponse createBlankInstance() {
        return new GetSnapshotsResponse();
    }

    @Override
    protected GetSnapshotsResponse createTestInstance() {
        ArrayList<SnapshotInfo> snapshots = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(5, 10); ++i) {
            SnapshotId snapshotId = new SnapshotId("snapshot " + i, UUIDs.base64UUID());
            String reason = randomBoolean() ? null : "reason";
            ShardId shardId = new ShardId("index", UUIDs.base64UUID(), 2);
            List<SnapshotShardFailure> shardFailures = Collections.singletonList(new SnapshotShardFailure("node-id", shardId, "reason"));
            snapshots.add(new SnapshotInfo(snapshotId, Arrays.asList("indice1", "indice2"), System.currentTimeMillis(), reason,
                System.currentTimeMillis(), randomIntBetween(2, 3), shardFailures, randomBoolean()));

        }
        return new GetSnapshotsResponse(snapshots);
    }
}
