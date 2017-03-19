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

package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class IndexResponseTests extends ESTestCase {

    public void testToXContent() {
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "type", "id", 5, true);
            String output = Strings.toString(indexResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":5,\"result\":\"created\",\"_shards\":null," +
                    "\"created\":true}", output);
        }
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "type", "id", 7, true);
            indexResponse.setForcedRefresh(true);
            indexResponse.setShardInfo(new ReplicationResponse.ShardInfo(10, 5));
            String output = Strings.toString(indexResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":7,\"result\":\"created\"," +
                    "\"forced_refresh\":true,\"_shards\":{\"total\":10,\"successful\":5,\"failed\":0},\"created\":true}", output);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        // Create a random IndexResponse and converts it to XContent in bytes
        IndexResponse indexResponse = randomIndexResponse();
        boolean humanReadable = randomBoolean();
        BytesReference indexResponseBytes = toXContent(indexResponse, xContentType, humanReadable);

        // Parse the XContent bytes to obtain a parsed
        IndexResponse parsedIndexResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), indexResponseBytes)) {
            parsedIndexResponse = IndexResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed index response
        // because the random index response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.

        // Print the parsed object out and test that the output is the same as the original output
        BytesReference parsedIndexResponseBytes = toXContent(parsedIndexResponse, xContentType, humanReadable);
        try (XContentParser parser = createParser(xContentType.xContent(), parsedIndexResponseBytes)) {
            assertIndexResponse(indexResponse, parser.map());
        }
    }

    @SuppressWarnings("unchecked")
    public static void assertDocWriteResponse(DocWriteResponse expected, Map<String, Object> actual) {
        assertEquals(expected.getIndex(), actual.get("_index"));
        assertEquals(expected.getType(), actual.get("_type"));
        assertEquals(expected.getId(), actual.get("_id"));
        assertEquals(expected.getVersion(), ((Number) actual.get("_version")).longValue());
        assertEquals(expected.getResult().getLowercase(), actual.get("result"));
        if (expected.forcedRefresh()) {
            assertTrue((Boolean) actual.get("forced_refresh"));
        } else {
            assertFalse(actual.containsKey("forced_refresh"));
        }

        Map<String, Object> actualShards = (Map<String, Object>) actual.get("_shards");
        assertNotNull(actualShards);
        assertEquals(expected.getShardInfo().getTotal(), actualShards.get("total"));
        assertEquals(expected.getShardInfo().getSuccessful(), actualShards.get("successful"));
        assertEquals(expected.getShardInfo().getFailed(), actualShards.get("failed"));

        List<Map<String, Object>> actualFailures = (List<Map<String, Object>>) actualShards.get("failures");
        if (CollectionUtils.isEmpty(expected.getShardInfo().getFailures())) {
            assertNull(actualFailures);
        } else {
            assertEquals(expected.getShardInfo().getFailures().length, actualFailures.size());
            for (int i = 0; i < expected.getShardInfo().getFailures().length; i++) {
                ReplicationResponse.ShardInfo.Failure failure = expected.getShardInfo().getFailures()[i];
                Map<String, Object> actualFailure = actualFailures.get(i);

                assertEquals(failure.index(), actualFailure.get("_index"));
                assertEquals(failure.shardId(), actualFailure.get("_shard"));
                assertEquals(failure.nodeId(), actualFailure.get("_node"));
                assertEquals(failure.status(), RestStatus.valueOf((String) actualFailure.get("status")));
                assertEquals(failure.primary(), actualFailure.get("primary"));

                Throwable cause = failure.getCause();
                Map<String, Object> actualClause = (Map<String, Object>) actualFailure.get("reason");
                assertNotNull(actualClause);
                while (cause != null) {
                    // The expected IndexResponse has been converted in XContent, then the resulting bytes have been
                    // parsed to create a new parsed IndexResponse. During this process, the type of the exceptions
                    // have been lost.
                    assertEquals("exception", actualClause.get("type"));
                    String expectedMessage = "Elasticsearch exception [type=" + ElasticsearchException.getExceptionName(cause)
                            + ", reason=" + cause.getMessage() + "]";
                    assertEquals(expectedMessage, actualClause.get("reason"));

                    if (cause instanceof ElasticsearchException) {
                        ElasticsearchException ex = (ElasticsearchException) cause;
                        Map<String, Object> actualHeaders = (Map<String, Object>) actualClause.get("header");

                        // When a IndexResponse is converted to XContent, the exception headers that start with "es."
                        // are added to the XContent as fields with the prefix removed. Other headers are added under
                        // a "header" root object.
                        // In the test, the "es." prefix is lost when the XContent is generating, so when the parsed
                        // IndexResponse is converted back to XContent all exception headers are under the "header" object.
                        for (String name : ex.getHeaderKeys()) {
                            assertEquals(ex.getHeader(name).get(0), actualHeaders.get(name.replaceFirst("es.", "")));
                        }
                    }
                    actualClause = (Map<String, Object>) actualClause.get("caused_by");
                    cause = cause.getCause();
                }
            }
        }
    }

    private static void assertIndexResponse(IndexResponse expected, Map<String, Object> actual) {
        assertDocWriteResponse(expected, actual);
        if (expected.getResult() == DocWriteResponse.Result.CREATED) {
            assertTrue((boolean) actual.get("created"));
        } else {
            assertFalse((boolean) actual.get("created"));
        }
    }

    private static IndexResponse randomIndexResponse() {
        ShardId shardId = new ShardId(randomAsciiOfLength(5), randomAsciiOfLength(5), randomIntBetween(0, 5));
        String type = randomAsciiOfLength(5);
        String id = randomAsciiOfLength(5);
        long version = randomBoolean() ? randomNonNegativeLong() : randomIntBetween(0, 10000);
        boolean created = randomBoolean();

        IndexResponse indexResponse = new IndexResponse(shardId, type, id, version, created);
        indexResponse.setForcedRefresh(randomBoolean());
        indexResponse.setShardInfo(RandomObjects.randomShardInfo(random(), randomBoolean()));
        return indexResponse;
    }

}
