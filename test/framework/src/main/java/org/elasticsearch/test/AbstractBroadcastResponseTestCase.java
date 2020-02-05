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

package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public abstract class AbstractBroadcastResponseTestCase<T extends BroadcastResponse> extends AbstractXContentTestCase<T> {

    @Override
    protected T createTestInstance() {
        int totalShards = randomIntBetween(1, 10);
        List<DefaultShardOperationFailedException> failures = null;
        int successfulShards = randomInt(totalShards);
        int failedShards = totalShards - successfulShards;
        if (failedShards > 0) {
            failures = new ArrayList<>();
            for (int i = 0; i < failedShards; i++) {
                ElasticsearchException exception = new ElasticsearchException("exception message " + i);
                String index = randomAlphaOfLengthBetween(3, 10);
                exception.setIndex(new Index(index, "_na_"));
                exception.setShard(new ShardId(index, "_na_", i));
                if (randomBoolean()) {
                    failures.add(new DefaultShardOperationFailedException(exception));
                } else {
                    failures.add(new DefaultShardOperationFailedException(index, i, new Exception("exception message " + i)));
                }
            }
        }
        return createTestInstance(totalShards, successfulShards, failedShards, failures);
    }

    protected abstract T createTestInstance(int totalShards, int successfulShards, int failedShards,
                                          List<DefaultShardOperationFailedException> failures);

    @Override
    protected void assertEqualInstances(T response, T parsedResponse) {
        assertThat(response.getTotalShards(), equalTo(parsedResponse.getTotalShards()));
        assertThat(response.getSuccessfulShards(), equalTo(parsedResponse.getSuccessfulShards()));
        assertThat(response.getFailedShards(), equalTo(parsedResponse.getFailedShards()));
        DefaultShardOperationFailedException[] originalFailures = response.getShardFailures();
        DefaultShardOperationFailedException[] parsedFailures = parsedResponse.getShardFailures();
        assertThat(originalFailures.length, equalTo(parsedFailures.length));
        for (int i = 0; i < originalFailures.length; i++) {
            assertThat(originalFailures[i].index(), equalTo(parsedFailures[i].index()));
            assertThat(originalFailures[i].shardId(), equalTo(parsedFailures[i].shardId()));
            assertThat(originalFailures[i].status(), equalTo(parsedFailures[i].status()));
            assertThat(parsedFailures[i].getCause().getMessage(), containsString(originalFailures[i].getCause().getMessage()));
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    public void testFailuresDeduplication() throws IOException {
        List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        Index index = new Index("test", "_na_");
        ElasticsearchException exception1 = new ElasticsearchException("foo", new IllegalArgumentException("bar"));
        exception1.setIndex(index);
        exception1.setShard(new ShardId(index, 0));
        ElasticsearchException exception2 = new ElasticsearchException("foo", new IllegalArgumentException("bar"));
        exception2.setIndex(index);
        exception2.setShard(new ShardId(index, 1));
        ElasticsearchException exception3 = new ElasticsearchException("fizz", new IllegalStateException("buzz"));
        exception3.setIndex(index);
        exception3.setShard(new ShardId(index, 2));
        failures.add(new DefaultShardOperationFailedException(exception1));
        failures.add(new DefaultShardOperationFailedException(exception2));
        failures.add(new DefaultShardOperationFailedException(exception3));

        T response = createTestInstance(10, 7, 3, failures);
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytesReference = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        T parsedResponse;
        try(XContentParser parser = createParser(xContentType.xContent(), bytesReference)) {
            parsedResponse = doParseInstance(parser);
            assertNull(parser.nextToken());
        }

        assertThat(parsedResponse.getShardFailures().length, equalTo(2));
        DefaultShardOperationFailedException[] parsedFailures = parsedResponse.getShardFailures();
        assertThat(parsedFailures[0].index(), equalTo("test"));
        assertThat(parsedFailures[0].shardId(), anyOf(equalTo(0), equalTo(1)));
        assertThat(parsedFailures[0].status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(parsedFailures[0].getCause().getMessage(), containsString("foo"));
        assertThat(parsedFailures[1].index(), equalTo("test"));
        assertThat(parsedFailures[1].shardId(), equalTo(2));
        assertThat(parsedFailures[1].status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(parsedFailures[1].getCause().getMessage(), containsString("fizz"));
    }

    public void testToXContent() {
        T response = createTestInstance(10, 10, 0, null);
        String output = Strings.toString(response);
        assertEquals("{\"_shards\":{\"total\":10,\"successful\":10,\"failed\":0}}", output);
    }
}
