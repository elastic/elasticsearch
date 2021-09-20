/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Map;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotFeatureInfo;
import org.elasticsearch.snapshots.SnapshotFeatureInfoTests;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_DETAILS_XCONTENT_PARAM;

public class GetSnapshotsResponseTests extends AbstractSerializingTestCase<GetSnapshotsResponse> {

    @Override
    protected GetSnapshotsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSnapshotsResponse.fromXContent(parser);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        // Explicitly include the index details, excluded by default, since this is required for a faithful round-trip
        return new ToXContent.MapParams(Map.of(INDEX_DETAILS_XCONTENT_PARAM, "true"));
    }

    @Override
    protected GetSnapshotsResponse createTestInstance() {
        ArrayList<SnapshotInfo> snapshots = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(5, 10); ++i) {
            SnapshotId snapshotId = new SnapshotId("snapshot " + i, UUIDs.base64UUID());
            String reason = randomBoolean() ? null : "reason";
            ShardId shardId = new ShardId("index", UUIDs.base64UUID(), 2);
            List<SnapshotShardFailure> shardFailures = Collections.singletonList(new SnapshotShardFailure("node-id", shardId, "reason"));
            List<SnapshotFeatureInfo> featureInfos = randomList(0, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo);
            snapshots.add(
                new SnapshotInfo(
                    new Snapshot(randomAlphaOfLength(5), snapshotId),
                    Arrays.asList("index1", "index2"),
                    Collections.singletonList("ds"),
                    featureInfos,
                    reason,
                    System.currentTimeMillis(),
                    randomIntBetween(2, 3),
                    shardFailures,
                    randomBoolean(),
                    SnapshotInfoTestUtils.randomUserMetadata(),
                    System.currentTimeMillis(),
                    SnapshotInfoTestUtils.randomIndexSnapshotDetails()
                )
            );
        }
        return new GetSnapshotsResponse(snapshots, Collections.emptyMap(), null, snapshots.size() + randomInt(), randomInt());
    }

    @Override
    protected Writeable.Reader<GetSnapshotsResponse> instanceReader() {
        return GetSnapshotsResponse::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Don't inject random fields into the custom snapshot metadata, because the metadata map is equality-checked after doing a
        // round-trip through xContent serialization/deserialization. Even though the rest of the object ignores unknown fields,
        // `metadata` doesn't ignore unknown fields (it just includes them in the parsed object, because the keys are arbitrary),
        // so any new fields added to the metadata before it gets deserialized that weren't in the serialized version will
        // cause the equality check to fail.
        //
        // Also don't inject random junk into the index details section, since this is keyed by index name but the values
        // are required to be a valid IndexSnapshotDetails
        //
        // The actual fields are nested in an array, so this regex matches fields with names of the form
        // `responses.0.snapshots.3.metadata`
        final Pattern metadataPattern = Pattern.compile("snapshots\\.\\d+\\.metadata.*");
        final Pattern indexDetailsPattern = Pattern.compile("snapshots\\.\\d+\\.index_details");
        return s -> metadataPattern.matcher(s).matches() || indexDetailsPattern.matcher(s).matches();
    }
}
