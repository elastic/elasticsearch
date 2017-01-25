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
import org.elasticsearch.common.collect.Tuple;
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
        final Tuple<UpdateResponse, UpdateResponse> tuple = randomUpdateResponse(xContentType);
        boolean humanReadable = randomBoolean();

        // Parse the XContent bytes to obtain a parsed UpdateResponse
        UpdateResponse parsedUpdateResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), toXContent(tuple.v1(), xContentType, humanReadable))) {
            parsedUpdateResponse = UpdateResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        final UpdateResponse expectedUpdateResponse = tuple.v2();
        try (XContentParser parser = createParser(xContentType.xContent(), toXContent(parsedUpdateResponse, xContentType, humanReadable))) {
            IndexResponseTests.assertDocWriteResponse(expectedUpdateResponse, parser.map());
        }
        assertEquals(expectedUpdateResponse.getGetResult(), parsedUpdateResponse.getGetResult());
    }

    private static Tuple<UpdateResponse, UpdateResponse> randomUpdateResponse(XContentType xContentType) {
        Tuple<GetResult, GetResult> getResults = GetResultTests.randomGetResult(xContentType);
        GetResult actualGetResult = getResults.v1();
        GetResult expectedGetResult = getResults.v2();

        ShardId shardId = new ShardId(actualGetResult.getIndex(), randomAsciiOfLength(5), randomIntBetween(0, 5));
        String type = actualGetResult.getType();
        String id = actualGetResult.getId();
        long version = actualGetResult.getVersion();
        DocWriteResponse.Result result = actualGetResult.isExists() ? DocWriteResponse.Result.UPDATED : DocWriteResponse.Result.NOT_FOUND;

        // We also want small number values (randomNonNegativeLong() tend to generate high numbers)
        // in order to catch some conversion error that happen between int/long after parsing.
        Long seqNo = randomFrom(randomNonNegativeLong(), (long) randomIntBetween(0, 10_000), null);

        UpdateResponse actual, expected;
        if (seqNo != null) {
            ReplicationResponse.ShardInfo shardInfo = RandomObjects.randomShardInfo(random(), true);
            actual = new UpdateResponse(shardInfo, shardId, type, id, seqNo, version, result);
            expected = new UpdateResponse(shardInfo, shardId, type, id, seqNo, version, result);

        } else {
            actual = new UpdateResponse(shardId, type, id, version, result);
            expected = new UpdateResponse(shardId, type, id, version, result);
        }

        if (actualGetResult.isExists()) {
            actual.setGetResult(actualGetResult);
        }

        if (expectedGetResult.isExists()) {
            expected.setGetResult(new GetResult(shardId.getIndexName(), type, id, version,
                    expectedGetResult.isExists(), expectedGetResult.internalSourceRef(), expectedGetResult.getFields()));
        }

        boolean forcedRefresh = randomBoolean();
        actual.setForcedRefresh(forcedRefresh);
        expected.setForcedRefresh(forcedRefresh);

        return Tuple.tuple(actual, expected);
    }
}
