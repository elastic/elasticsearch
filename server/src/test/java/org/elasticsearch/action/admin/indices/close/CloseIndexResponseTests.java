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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.transport.ActionNotFoundTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CloseIndexResponseTests extends AbstractWireSerializingTestCase<CloseIndexResponse> {

    @Override
    protected CloseIndexResponse createTestInstance() {
        return randomResponse();
    }

    @Override
    protected Writeable.Reader<CloseIndexResponse> instanceReader() {
        return CloseIndexResponse::new;
    }

    @Override
    protected void assertEqualInstances(CloseIndexResponse expected, CloseIndexResponse actual) {
        assertNotSame(expected, actual);
        assertThat(actual.isAcknowledged(), equalTo(expected.isAcknowledged()));
        assertThat(actual.isShardsAcknowledged(), equalTo(expected.isShardsAcknowledged()));

        for (int i = 0; i < expected.getIndices().size(); i++) {
            CloseIndexResponse.IndexResult expectedIndexResult = expected.getIndices().get(i);
            CloseIndexResponse.IndexResult actualIndexResult = actual.getIndices().get(i);
            assertNotSame(expectedIndexResult, actualIndexResult);
            assertThat(actualIndexResult.getIndex(), equalTo(expectedIndexResult.getIndex()));
            assertThat(actualIndexResult.hasFailures(), equalTo(expectedIndexResult.hasFailures()));

            if (expectedIndexResult.hasFailures() == false) {
                assertThat(actualIndexResult.getException(), nullValue());
                if (actualIndexResult.getShards() != null) {
                    assertThat(Arrays.stream(actualIndexResult.getShards())
                        .allMatch(shardResult -> shardResult.hasFailures() == false), is(true));
                }
            }

            if (expectedIndexResult.getException() != null) {
                assertThat(actualIndexResult.getShards(), nullValue());
                assertThat(actualIndexResult.getException(), notNullValue());
                assertThat(actualIndexResult.getException().getMessage(), equalTo(expectedIndexResult.getException().getMessage()));
                assertThat(actualIndexResult.getException().getClass(), equalTo(expectedIndexResult.getException().getClass()));
                assertArrayEquals(actualIndexResult.getException().getStackTrace(), expectedIndexResult.getException().getStackTrace());
            } else {
                assertThat(actualIndexResult.getException(), nullValue());
            }

            if (expectedIndexResult.getShards() != null) {
                assertThat(actualIndexResult.getShards().length, equalTo(expectedIndexResult.getShards().length));

                for (int j = 0; j < expectedIndexResult.getShards().length; j++) {
                    CloseIndexResponse.ShardResult expectedShardResult = expectedIndexResult.getShards()[j];
                    CloseIndexResponse.ShardResult actualShardResult = actualIndexResult.getShards()[j];
                    assertThat(actualShardResult.getId(), equalTo(expectedShardResult.getId()));
                    assertThat(actualShardResult.hasFailures(), equalTo(expectedShardResult.hasFailures()));

                    if (expectedShardResult.hasFailures()) {
                        assertThat(actualShardResult.getFailures().length, equalTo(expectedShardResult.getFailures().length));

                        for (int k = 0; k < expectedShardResult.getFailures().length; k++) {
                            CloseIndexResponse.ShardResult.Failure expectedFailure = expectedShardResult.getFailures()[k];
                            CloseIndexResponse.ShardResult.Failure actualFailure = actualShardResult.getFailures()[k];
                            assertThat(actualFailure.getNodeId(), equalTo(expectedFailure.getNodeId()));
                            assertThat(actualFailure.index(), equalTo(expectedFailure.index()));
                            assertThat(actualFailure.shardId(), equalTo(expectedFailure.shardId()));
                            assertThat(actualFailure.reason(), equalTo(expectedFailure.reason()));
                            assertThat(actualFailure.getCause().getMessage(), equalTo(expectedFailure.getCause().getMessage()));
                            assertThat(actualFailure.getCause().getClass(), equalTo(expectedFailure.getCause().getClass()));
                            assertArrayEquals(actualFailure.getCause().getStackTrace(), expectedFailure.getCause().getStackTrace());
                        }
                    } else {
                        assertThat(actualShardResult.getFailures(), nullValue());
                    }
                }
            } else {
                assertThat(actualIndexResult.getShards(), nullValue());
            }
        }
    }

    /**
     * Test that random responses can be written to xcontent without errors.
     * Also check some specific simple cases for output.
     */
    public void testToXContent() throws IOException {
        CloseIndexResponse response = randomResponse();
        XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        Index index = new Index("test", "uuid");
        IndexResult indexResult = new CloseIndexResponse.IndexResult(index);
        CloseIndexResponse closeIndexResponse = new CloseIndexResponse(true, true,
                Collections.singletonList(indexResult));
        assertEquals("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":{\"test\":{\"closed\":true}}}",
                Strings.toString(closeIndexResponse));

        CloseIndexResponse.ShardResult[] shards = new CloseIndexResponse.ShardResult[1];
        shards[0] = new CloseIndexResponse.ShardResult(0, new CloseIndexResponse.ShardResult.Failure[] {
                new CloseIndexResponse.ShardResult.Failure("test", 0, new ActionNotFoundTransportException("test"), "nodeId") });
        indexResult = new CloseIndexResponse.IndexResult(index, shards);
        closeIndexResponse = new CloseIndexResponse(true, true,
                Collections.singletonList(indexResult));
        assertEquals("{\"acknowledged\":true,\"shards_acknowledged\":true,"
                + "\"indices\":{\"test\":{\"closed\":false,\"failedShards\":{\"0\":{"
                + "\"failures\":[{\"node\":\"nodeId\",\"shard\":0,\"index\":\"test\",\"status\":\"INTERNAL_SERVER_ERROR\","
                + "\"reason\":{\"type\":\"action_not_found_transport_exception\","
                + "\"reason\":\"No handler for action [test]\"}}]}}}}}",
                Strings.toString(closeIndexResponse));
    }

    public void testBwcSerialization() throws Exception {
        {
            final CloseIndexResponse response = randomResponse();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(randomVersionBetween(random(), Version.V_7_0_0, getPreviousVersion(Version.V_7_2_0)));
                response.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(out.getVersion());
                    final AcknowledgedResponse deserializedResponse = new AcknowledgedResponse(in);
                    assertThat(deserializedResponse.isAcknowledged(), equalTo(response.isAcknowledged()));
                }
            }
        }
        {
            final AcknowledgedResponse response = new AcknowledgedResponse(randomBoolean());
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                response.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(randomVersionBetween(random(), Version.V_7_0_0, getPreviousVersion(Version.V_7_2_0)));
                    final CloseIndexResponse deserializedResponse = new CloseIndexResponse(in);
                    assertThat(deserializedResponse.isAcknowledged(), equalTo(response.isAcknowledged()));
                }
            }
        }
        {
            final CloseIndexResponse response = randomResponse();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                Version version = randomVersionBetween(random(), Version.V_7_2_0, Version.CURRENT);
                out.setVersion(version);
                response.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    final CloseIndexResponse deserializedResponse = new CloseIndexResponse(in);
                    assertThat(deserializedResponse.isAcknowledged(), equalTo(response.isAcknowledged()));
                    assertThat(deserializedResponse.isShardsAcknowledged(), equalTo(response.isShardsAcknowledged()));
                    if (version.onOrAfter(Version.V_7_3_0)) {
                        assertThat(deserializedResponse.getIndices(), hasSize(response.getIndices().size()));
                    } else {
                        assertThat(deserializedResponse.getIndices(), empty());
                    }
                }
            }
        }
    }

    private CloseIndexResponse randomResponse() {
        boolean acknowledged = true;
        final String[] indicesNames = generateRandomStringArray(10, 10, false, true);

        final List<CloseIndexResponse.IndexResult> indexResults = new ArrayList<>();
        for (String indexName : indicesNames) {
            final Index index = new Index(indexName, randomAlphaOfLength(5));
            if (randomBoolean()) {
                indexResults.add(new CloseIndexResponse.IndexResult(index));
            } else {
                if (randomBoolean()) {
                    acknowledged = false;
                    indexResults.add(new CloseIndexResponse.IndexResult(index, randomException(index, 0)));
                } else {
                    final int nbShards = randomIntBetween(1, 5);
                    CloseIndexResponse.ShardResult[] shards = new CloseIndexResponse.ShardResult[nbShards];
                    for (int i = 0; i < nbShards; i++) {
                        CloseIndexResponse.ShardResult.Failure[] failures = null;
                        if (randomBoolean()) {
                            acknowledged = false;
                            failures = new CloseIndexResponse.ShardResult.Failure[randomIntBetween(1, 3)];
                            for (int j = 0; j < failures.length; j++) {
                                String nodeId = null;
                                if (frequently()) {
                                    nodeId = randomAlphaOfLength(5);
                                }
                                failures[j] = new CloseIndexResponse.ShardResult.Failure(indexName, i, randomException(index, i), nodeId);
                            }
                        }
                        shards[i] = new CloseIndexResponse.ShardResult(i, failures);
                    }
                    indexResults.add(new CloseIndexResponse.IndexResult(index, shards));
                }
            }
        }

        final boolean shardsAcknowledged = acknowledged ? randomBoolean() : false;
        return new CloseIndexResponse(acknowledged, shardsAcknowledged, indexResults);
    }

    private static ElasticsearchException randomException(final Index index, final int id) {
        return randomFrom(
            new IndexNotFoundException(index),
            new ActionNotFoundTransportException("test"),
            new NoShardAvailableActionException(new ShardId(index, id)));
    }
}
