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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.CoreMatchers.containsString;

public class GetSnapshotsResponseTests extends ESTestCase {
    // We can not subclass AbstractStreamableXContentTestCase because it
    // can only be used for instances with equals and hashCode
    // GetSnapshotResponse does not override equals and hashCode.
    // It does not override equals and hashCode, because it
    // contains ElasticsearchException, which does not override equals and hashCode.

    private GetSnapshotsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSnapshotsResponse.fromXContent(parser);
    }

    private GetSnapshotsResponse copyInstance(GetSnapshotsResponse instance, Version version) throws IOException {
        return copyStreamable(instance, new NamedWriteableRegistry(Collections.emptyList()), () -> new GetSnapshotsResponse(), version);
    }

    private void assertEqualInstances(GetSnapshotsResponse expectedInstance, GetSnapshotsResponse newInstance) {
        assertEquals(expectedInstance.getSuccessfulResponses(), newInstance.getSuccessfulResponses());
        assertEquals(expectedInstance.getFailedResponses().keySet(), newInstance.getFailedResponses().keySet());
        for (Map.Entry<String, ElasticsearchException> expectedEntry : expectedInstance.getFailedResponses().entrySet()) {
            ElasticsearchException expectedException = expectedEntry.getValue();
            ElasticsearchException newException = newInstance.getFailedResponses().get(expectedEntry.getKey());
            assertThat(newException.getMessage(), containsString(expectedException.getMessage()));
        }
    }

    private List<SnapshotInfo> createSnapshotInfos() {
        ArrayList<SnapshotInfo> snapshots = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(5, 10); ++i) {
            SnapshotId snapshotId = new SnapshotId("snapshot " + i, UUIDs.base64UUID());
            String reason = randomBoolean() ? null : "reason";
            ShardId shardId = new ShardId("index", UUIDs.base64UUID(), 2);
            List<SnapshotShardFailure> shardFailures = Collections.singletonList(new SnapshotShardFailure("node-id", shardId, "reason"));
            snapshots.add(new SnapshotInfo(snapshotId, Arrays.asList("index1", "index2"), System.currentTimeMillis(), reason,
                    System.currentTimeMillis(), randomIntBetween(2, 3), shardFailures, randomBoolean()));

        }
        return snapshots;
    }

    private GetSnapshotsResponse createTestInstance() {
        Set<String> repositories = new HashSet<>();
        Map<String, List<SnapshotInfo>> successfulResponses = new HashMap<>();
        Map<String, ElasticsearchException> failedResponses = new HashMap<>();

        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String repository = randomValueOtherThanMany(r -> repositories.contains(r), () -> randomAlphaOfLength(10));
            repositories.add(repository);
            successfulResponses.put(repository, createSnapshotInfos());
        }

        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String repository = randomValueOtherThanMany(r -> repositories.contains(r), () -> randomAlphaOfLength(10));
            repositories.add(repository);
            failedResponses.put(repository, new ElasticsearchException(randomAlphaOfLength(10)));
        }

        return new GetSnapshotsResponse(successfulResponses, failedResponses);
    }

    public void testSerialization() throws IOException {
        GetSnapshotsResponse testInstance = createTestInstance();
        GetSnapshotsResponse deserializedInstance = copyInstance(testInstance, Version.CURRENT);
        assertEqualInstances(testInstance, deserializedInstance);
    }

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, ToXContent.EMPTY_PARAMS, this::doParseInstance)
                .numberOfTestRuns(1)
                .supportsUnknownFields(true)
                .shuffleFieldsExceptions(Strings.EMPTY_ARRAY)
                .randomFieldsExcludeFilter(field -> false)
                .assertEqualsConsumer(this::assertEqualInstances)
                // We set it to false, because GetSnapshotsResponse contains
                // ElasticsearchException, whose xContent creation/parsing are not stable.
                .assertToXContentEquivalence(false)
                .test();
    }

}
