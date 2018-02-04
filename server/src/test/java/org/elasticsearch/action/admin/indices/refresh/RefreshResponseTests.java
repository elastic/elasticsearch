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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.equalTo;

public class RefreshResponseTests extends ESTestCase {

    private static List<DefaultShardOperationFailedException> failures = new ArrayList<>();

    private static class FakeElasticsearchException extends ElasticsearchException {

        private Index index;
        private ShardId shardId;
        private RestStatus status;

        public FakeElasticsearchException(String index, int shardId, RestStatus status, String msg) {
            super(msg);
            this.index = new Index(index, "_na_");
            this.shardId = new ShardId(this.index, shardId);
            this.status = status;
        }

        @Override
        public Index getIndex() {
            return this.index;
        }

        @Override
        public ShardId getShardId() {
            return this.shardId;
        }

        @Override
        public RestStatus status() {
            return this.status;
        }
    }

    @BeforeClass
    public static void prepareException() {
        failures = new ArrayList<>();
        failures.add(new DefaultShardOperationFailedException(
            new FakeElasticsearchException("index1", 1, RestStatus.INTERNAL_SERVER_ERROR, "fake exception 1")));
        failures.add(new DefaultShardOperationFailedException(
            new FakeElasticsearchException("index2", 2, RestStatus.GATEWAY_TIMEOUT, "fake exception 2")));
    }

    public void testToXContent() {
        RefreshResponse response = new RefreshResponse(10, 10, 0, null);
        String output = Strings.toString(response);
        assertEquals("{\"_shards\":{\"total\":10,\"successful\":10,\"failed\":0}}", output);
    }

    public void testToAndFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        RefreshResponse response = new RefreshResponse(10, 10, 0, failures);
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytesReference = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        if (addRandomFields) {
            bytesReference = insertRandomFields(xContentType, bytesReference, null, random());
        }
        RefreshResponse parsedResponse;
        try(XContentParser parser = createParser(xContentType.xContent(), bytesReference)) {
            parsedResponse = RefreshResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertThat(response.getTotalShards(), equalTo(parsedResponse.getTotalShards()));
        assertThat(response.getSuccessfulShards(), equalTo(parsedResponse.getSuccessfulShards()));
        assertThat(response.getFailedShards(), equalTo(parsedResponse.getFailedShards()));
        assertThat(response.getShardFailures(), equalTo(parsedResponse.getShardFailures()));
    }
}
