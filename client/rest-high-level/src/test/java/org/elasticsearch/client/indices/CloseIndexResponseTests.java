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
package org.elasticsearch.client.indices;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.ActionNotFoundTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CloseIndexResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.action.admin.indices.close.CloseIndexResponse, CloseIndexResponse> {

    @Override
    protected org.elasticsearch.action.admin.indices.close.CloseIndexResponse createServerTestInstance(XContentType xContentType) {
        boolean acknowledged = true;
        final String[] indicesNames = generateRandomStringArray(10, 10, false, true);

        final List<org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult> indexResults = new ArrayList<>();
        for (String indexName : indicesNames) {
            final Index index = new Index(indexName, randomAlphaOfLength(5));
            if (randomBoolean()) {
                indexResults.add(new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult(index));
            } else {
                if (randomBoolean()) {
                    acknowledged = false;
                    Exception exception = randomFrom(new IndexNotFoundException(index), new ActionNotFoundTransportException("test"));
                    indexResults.add(new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult(index, exception));
                } else {
                    final int nbShards = randomIntBetween(1, 5);
                    org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult[] shards =
                        new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult[nbShards];
                    for (int i = 0; i < nbShards; i++) {
                        org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult.Failure[] failures = null;
                        if (randomBoolean()) {
                            acknowledged = false;
                            int nbFailures = randomIntBetween(1, 3);
                            failures = new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult.Failure[nbFailures];
                            for (int j = 0; j < failures.length; j++) {
                                String nodeId = null;
                                if (frequently()) {
                                    nodeId = randomAlphaOfLength(5);
                                }
                                failures[j] = newFailure(indexName, i, nodeId);
                            }
                        }
                        shards[i] = new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult(i, failures);
                    }
                    indexResults.add(new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult(index, shards));
                }
            }
        }

        final boolean shardsAcknowledged = acknowledged ? randomBoolean() : false;
        return new org.elasticsearch.action.admin.indices.close.CloseIndexResponse(acknowledged, shardsAcknowledged, indexResults);
    }

    @Override
    protected CloseIndexResponse doParseToClientInstance(final XContentParser parser) throws IOException {
        return CloseIndexResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(final org.elasticsearch.action.admin.indices.close.CloseIndexResponse serverInstance,
                                   final CloseIndexResponse clientInstance) {
        assertNotSame(serverInstance, clientInstance);
        assertThat(clientInstance.isAcknowledged(), equalTo(serverInstance.isAcknowledged()));
        assertThat(clientInstance.isShardsAcknowledged(), equalTo(serverInstance.isShardsAcknowledged()));

        assertThat(clientInstance.getIndices(), hasSize(serverInstance.getIndices().size()));
        serverInstance.getIndices().forEach(expectedIndexResult -> {
            List<CloseIndexResponse.IndexResult> actualIndexResults = clientInstance.getIndices().stream()
                .filter(result -> result.getIndex().equals(expectedIndexResult.getIndex().getName()))
                .collect(Collectors.toList());
            assertThat(actualIndexResults, hasSize(1));

            final CloseIndexResponse.IndexResult actualIndexResult = actualIndexResults.get(0);
            assertThat(actualIndexResult.hasFailures(), equalTo(expectedIndexResult.hasFailures()));

            if (expectedIndexResult.hasFailures() == false) {
                assertThat(actualIndexResult.getException(), nullValue());
                assertThat(actualIndexResult.getShards(), nullValue());
            }
            if (expectedIndexResult.getException() != null) {
                assertThat(actualIndexResult.hasFailures(), is(true));
                assertThat(actualIndexResult.getShards(), nullValue());
                assertThat(actualIndexResult.getException(), notNullValue());
                assertThat(actualIndexResult.getException().getMessage(), containsString(expectedIndexResult.getException().getMessage()));
            }
            if (expectedIndexResult.getShards() != null) {
                assertThat(actualIndexResult.getException(), nullValue());

                List<org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult> failedShardResults =
                    Arrays.stream(expectedIndexResult.getShards())
                        .filter(org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult::hasFailures)
                        .collect(Collectors.toList());

                if (failedShardResults.isEmpty()) {
                    assertThat(actualIndexResult.hasFailures(), is(false));
                    assertThat(actualIndexResult.getShards(), nullValue());
                    return;
                }

                assertThat(actualIndexResult.hasFailures(), is(true));
                assertThat(actualIndexResult.getShards(), notNullValue());
                assertThat(actualIndexResult.getShards().length, equalTo(failedShardResults.size()));

                failedShardResults.forEach(failedShardResult -> {
                    List<CloseIndexResponse.ShardResult> actualShardResults = Arrays.stream(actualIndexResult.getShards())
                        .filter(result -> result.getId() == failedShardResult.getId())
                        .collect(Collectors.toList());
                    assertThat(actualShardResults, hasSize(1));

                    final CloseIndexResponse.ShardResult actualShardResult = actualShardResults.get(0);
                    assertThat(actualShardResult.hasFailures(), is(true));
                    assertThat(actualShardResult.getFailures(), notNullValue());
                    assertThat(actualShardResult.getFailures().length, equalTo(failedShardResult.getFailures().length));

                    for (int i = 0; i < failedShardResult.getFailures().length; i++) {
                        org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult.Failure expectedFailure =
                            failedShardResult.getFailures()[i];
                        CloseIndexResponse.ShardResult.Failure actualFailure = actualShardResult.getFailures()[i];
                        assertThat(actualFailure.getNodeId(), equalTo(expectedFailure.getNodeId()));
                        assertThat(actualFailure.index(), equalTo(expectedFailure.index()));
                        assertThat(actualFailure.shardId(), equalTo(expectedFailure.shardId()));
                        assertThat(actualFailure.getCause().getMessage(), containsString(expectedFailure.getCause().getMessage()));
                    }
                });
            }
        });
    }

    public final void testBwcFromXContent() throws IOException {
        {
            final boolean acknowledged = randomBoolean();
            final AcknowledgedResponse expected = new AcknowledgedResponse(acknowledged);

            final XContentType xContentType = randomFrom(XContentType.values());
            final BytesReference bytes = toShuffledXContent(expected, xContentType, getParams(), randomBoolean());
            final XContent xContent = XContentFactory.xContent(xContentType);
            final XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                bytes.streamInput());

            final CloseIndexResponse actual = doParseToClientInstance(parser);
            assertThat(actual, notNullValue());
            assertThat(actual.isAcknowledged(), equalTo(expected.isAcknowledged()));
            assertThat(actual.isShardsAcknowledged(), equalTo(expected.isAcknowledged()));
            assertThat(actual.getIndices(), hasSize(0));
        }
        {
            final boolean acknowledged = randomBoolean();
            final boolean shardsAcknowledged = acknowledged ? randomBoolean() : false;
            final ShardsAcknowledgedResponse expected = new ShardsAcknowledgedResponse(acknowledged, shardsAcknowledged){};

            final XContentType xContentType = randomFrom(XContentType.values());
            final BytesReference bytes = toShuffledXContent(expected, xContentType, getParams(), randomBoolean());
            final XContent xContent = XContentFactory.xContent(xContentType);
            final XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                bytes.streamInput());

            final CloseIndexResponse actual = doParseToClientInstance(parser);
            assertThat(actual, notNullValue());
            assertThat(actual.isAcknowledged(), equalTo(expected.isAcknowledged()));
            assertThat(actual.isShardsAcknowledged(), equalTo(expected.isShardsAcknowledged()));
            assertThat(actual.getIndices(), hasSize(0));
        }
    }

    private org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult.Failure newFailure(final String indexName,
                                                                                                           final int shard,
                                                                                                           final String nodeId) {
        Exception exception = randomFrom(new IndexNotFoundException(indexName),
            new ActionNotFoundTransportException("test"),
            new IOException("boom", new NullPointerException()),
            new ElasticsearchStatusException("something", RestStatus.TOO_MANY_REQUESTS));
        return new org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult.Failure(indexName, shard, exception, nodeId);
    }
}
