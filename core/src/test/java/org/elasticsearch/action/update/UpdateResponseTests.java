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

package org.elasticsearch.action.update;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.DocWriteResponse.Result.DELETED;
import static org.elasticsearch.action.DocWriteResponse.Result.NOT_FOUND;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class UpdateResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        {
            UpdateResponse updateResponse = new UpdateResponse(new ShardId("index", "index_uuid", 0), "type", "id", 0, NOT_FOUND);
            String output = Strings.toString(updateResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":0,\"result\":\"not_found\"," +
                    "\"_shards\":{\"total\":0,\"successful\":0,\"failed\":0}}", output);
        }
        {
            UpdateResponse updateResponse = new UpdateResponse(new ReplicationResponse.ShardInfo(10, 6),
                    new ShardId("index", "index_uuid", 1), "type", "id", 3, 1, DELETED);
            String output = Strings.toString(updateResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":1,\"result\":\"deleted\"," +
                    "\"_shards\":{\"total\":10,\"successful\":6,\"failed\":0},\"_seq_no\":3}", output);
        }
        {
            BytesReference source = new BytesArray("{\"title\":\"Book title\",\"isbn\":\"ABC-123\"}");
            Map<String, GetField> fields = new HashMap<>();
            fields.put("title", new GetField("title", Collections.singletonList("Book title")));
            fields.put("isbn", new GetField("isbn", Collections.singletonList("ABC-123")));

            UpdateResponse updateResponse = new UpdateResponse(new ReplicationResponse.ShardInfo(3, 2),
                    new ShardId("books", "books_uuid", 2), "book", "1", 7, 2, UPDATED);
            updateResponse.setGetResult(new GetResult("books", "book", "1", 2, true, source, fields));

            String output = Strings.toString(updateResponse);
            assertEquals("{\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"1\",\"_version\":2,\"result\":\"updated\"," +
                    "\"_shards\":{\"total\":3,\"successful\":2,\"failed\":0},\"_seq_no\":7,\"get\":{\"found\":true," +
                    "\"_source\":{\"title\":\"Book title\",\"isbn\":\"ABC-123\"},\"fields\":{\"isbn\":[\"ABC-123\"],\"title\":[\"Book " +
                    "title\"]}}}", output);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        // Create a random UpdateResponse and converts it to XContent in bytes
        UpdateResponse updateResponse = randomUpdateResponse();
        BytesReference updateResponseBytes = toXContent(updateResponse, xContentType);

        // Parse the XContent bytes to obtain a parsed UpdateResponse
        UpdateResponse parsedUpdateResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), updateResponseBytes)) {
            parsedUpdateResponse = UpdateResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed update response
        // because the random update response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.

        // Print the parsed object out and test that the output is the same as the original output
        BytesReference parsedUpdateResponseBytes = toXContent(parsedUpdateResponse, xContentType);
        try (XContentParser parser = createParser(xContentType.xContent(), parsedUpdateResponseBytes)) {
            assertUpdateResponse(updateResponse, parser.map());
        }
    }

    private static void assertUpdateResponse(UpdateResponse expected, Map<String, Object> actual) {
        IndexResponseTests.assertDocWriteResponse(expected, actual);
    }

    private static UpdateResponse randomUpdateResponse() {
        ShardId shardId = new ShardId(randomAsciiOfLength(5), randomAsciiOfLength(5), randomIntBetween(0, 5));
        String type = randomAsciiOfLength(5);
        String id = randomAsciiOfLength(5);
        DocWriteResponse.Result result = randomFrom(DocWriteResponse.Result.values());
        long version = (long) randomIntBetween(0, 5);
        boolean forcedRefresh = randomBoolean();

        UpdateResponse updateResponse = null;
        if (rarely()) {
            updateResponse = new UpdateResponse(shardId, type, id, version, result);
        } else {
            long seqNo = randomIntBetween(0, 5);
            ReplicationResponse.ShardInfo shardInfo = RandomObjects.randomShardInfo(random(), randomBoolean());
            updateResponse = new UpdateResponse(shardInfo, shardId, type, id, seqNo, version, result);

            GetResult getResult = GetResultTests.randomGetResult(randomFrom(XContentType.values())).v1();
            updateResponse.setGetResult(getResult);
        }
        updateResponse.setForcedRefresh(forcedRefresh);
        return updateResponse;
    }
}
