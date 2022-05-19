/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotFeatureInfo;
import org.elasticsearch.snapshots.SnapshotFeatureInfoTests;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_DETAILS_XCONTENT_PARAM;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.CoreMatchers.containsString;

public class GetSnapshotsResponseTests extends ESTestCase {
    // We can not subclass AbstractSerializingTestCase because it
    // can only be used for instances with equals and hashCode
    // GetSnapshotResponse does not override equals and hashCode.
    // It does not override equals and hashCode, because it
    // contains ElasticsearchException, which does not override equals and hashCode.

    private GetSnapshotsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSnapshotsResponse.fromXContent(parser);
    }

    private GetSnapshotsResponse copyInstance(GetSnapshotsResponse instance) throws IOException {
        return copyInstance(
            instance,
            new NamedWriteableRegistry(Collections.emptyList()),
            (out, value) -> value.writeTo(out),
            GetSnapshotsResponse::new,
            Version.CURRENT
        );

    }

    private void assertEqualInstances(GetSnapshotsResponse expectedInstance, GetSnapshotsResponse newInstance) {
        assertEquals(expectedInstance.getSnapshots(), newInstance.getSnapshots());
        assertEquals(expectedInstance.next(), newInstance.next());
        assertEquals(expectedInstance.getFailures().keySet(), newInstance.getFailures().keySet());
        for (Map.Entry<String, ElasticsearchException> expectedEntry : expectedInstance.getFailures().entrySet()) {
            ElasticsearchException expectedException = expectedEntry.getValue();
            ElasticsearchException newException = newInstance.getFailures().get(expectedEntry.getKey());
            assertThat(newException.getMessage(), containsString(expectedException.getMessage()));
        }
    }

    private List<SnapshotInfo> createSnapshotInfos(String repoName) {
        ArrayList<SnapshotInfo> snapshots = new ArrayList<>();
        final int targetSize = between(5, 10);
        for (int i = 0; i < targetSize; ++i) {
            SnapshotId snapshotId = new SnapshotId("snapshot " + i, UUIDs.base64UUID());
            String reason = randomBoolean() ? null : "reason";
            ShardId shardId = new ShardId("index", UUIDs.base64UUID(), 2);
            List<SnapshotShardFailure> shardFailures = Collections.singletonList(new SnapshotShardFailure("node-id", shardId, "reason"));
            List<SnapshotFeatureInfo> featureInfos = randomList(5, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo);
            snapshots.add(
                new SnapshotInfo(
                    new Snapshot(repoName, snapshotId),
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
        return snapshots;
    }

    private GetSnapshotsResponse createTestInstance() {
        Set<String> repositories = new HashSet<>();
        Map<String, ElasticsearchException> failures = new HashMap<>();
        List<SnapshotInfo> responses = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String repository = randomValueOtherThanMany(repositories::contains, () -> randomAlphaOfLength(10));
            repositories.add(repository);
            responses.addAll(createSnapshotInfos(repository));
        }

        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String repository = randomValueOtherThanMany(repositories::contains, () -> randomAlphaOfLength(10));
            repositories.add(repository);
            failures.put(repository, new ElasticsearchException(randomAlphaOfLength(10)));
        }

        return new GetSnapshotsResponse(
            responses,
            failures,
            randomBoolean()
                ? Base64.getUrlEncoder()
                    .encodeToString(
                        (randomAlphaOfLengthBetween(1, 5) + "," + randomAlphaOfLengthBetween(1, 5) + "," + randomAlphaOfLengthBetween(1, 5))
                            .getBytes(StandardCharsets.UTF_8)
                    )
                : null,
            randomIntBetween(responses.size(), responses.size() + 100),
            randomIntBetween(0, 100)
        );
    }

    public void testSerialization() throws IOException {
        GetSnapshotsResponse testInstance = createTestInstance();
        GetSnapshotsResponse deserializedInstance = copyInstance(testInstance);
        assertEqualInstances(testInstance, deserializedInstance);
    }

    public void testFromXContent() throws IOException {
        // Explicitly include the index details, excluded by default, since this is required for a faithful round-trip
        final ToXContent.MapParams params = new ToXContent.MapParams(Map.of(INDEX_DETAILS_XCONTENT_PARAM, "true"));

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
        // `snapshots.3.metadata`
        final Predicate<String> predicate = Pattern.compile("snapshots\\.\\d+\\.metadata.*")
            .asMatchPredicate()
            .or(Pattern.compile("snapshots\\.\\d+\\.index_details").asMatchPredicate())
            .or(Pattern.compile("failures\\.*").asMatchPredicate());
        xContentTester(this::createParser, this::createTestInstance, params, this::doParseInstance).numberOfTestRuns(1)
            .supportsUnknownFields(true)
            .shuffleFieldsExceptions(Strings.EMPTY_ARRAY)
            .randomFieldsExcludeFilter(predicate)
            .assertEqualsConsumer(this::assertEqualInstances)
            // We set it to false, because GetSnapshotsResponse contains
            // ElasticsearchException, whose xContent creation/parsing are not stable.
            .assertToXContentEquivalence(false)
            .test();
    }

}
