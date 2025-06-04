/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    protected CloseIndexResponse mutateInstance(CloseIndexResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
                    assertThat(
                        Arrays.stream(actualIndexResult.getShards()).allMatch(shardResult -> shardResult.hasFailures() == false),
                        is(true)
                    );
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

                            // Serialising and deserialising an exception seems to remove the "java.base/" part from the stack trace
                            // in the `reason` property, so we don't compare it directly. Instead, check that the first lines match,
                            // and that the stack trace has the same number of lines.
                            List<String> expectedReasonLines = expectedFailure.reason().lines().toList();
                            List<String> actualReasonLines = actualFailure.reason().lines().toList();
                            assertThat(actualReasonLines.get(0), equalTo(expectedReasonLines.get(0)));
                            assertThat(
                                "Exceptions have a different number of lines",
                                actualReasonLines,
                                hasSize(expectedReasonLines.size())
                            );

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
        CloseIndexResponse closeIndexResponse = new CloseIndexResponse(true, true, Collections.singletonList(indexResult));
        assertEquals("""
            {"acknowledged":true,"shards_acknowledged":true,"indices":{"test":{"closed":true}}}""", Strings.toString(closeIndexResponse));

        CloseIndexResponse.ShardResult[] shards = new CloseIndexResponse.ShardResult[1];
        shards[0] = new CloseIndexResponse.ShardResult(
            0,
            new CloseIndexResponse.ShardResult.Failure[] {
                new CloseIndexResponse.ShardResult.Failure("test", 0, new ActionNotFoundTransportException("test"), "nodeId") }
        );
        indexResult = new CloseIndexResponse.IndexResult(index, shards);
        closeIndexResponse = new CloseIndexResponse(true, true, Collections.singletonList(indexResult));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "acknowledged": true,
              "shards_acknowledged": true,
              "indices": {
                "test": {
                  "closed": false,
                  "failedShards": {
                    "0": {
                      "failures": [
                        {
                          "node": "nodeId",
                          "shard": 0,
                          "index": "test",
                          "status": "INTERNAL_SERVER_ERROR",
                          "reason": {
                            "type": "action_not_found_transport_exception",
                            "reason": "No handler for action [test]"
                          }
                        }
                      ]
                    }
                  }
                }
              }
            }"""), Strings.toString(closeIndexResponse));
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
            new NoShardAvailableActionException(new ShardId(index, id))
        );
    }
}
