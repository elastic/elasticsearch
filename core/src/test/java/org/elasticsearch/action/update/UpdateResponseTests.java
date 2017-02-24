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
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_UUID_NA_VALUE;
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
        UpdateResponse updateResponse = tuple.v1();
        UpdateResponse expectedUpdateResponse = tuple.v2();

        boolean humanReadable = randomBoolean();
        BytesReference updateResponseBytes = toXContent(updateResponse, xContentType, humanReadable);

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), updateResponseBytes)) {
                updateResponseBytes = shuffleXContent(parser, randomBoolean()).bytes();
            }
        }

        // Parse the XContent bytes to obtain a parsed UpdateResponse
        UpdateResponse parsedUpdateResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), updateResponseBytes)) {
            parsedUpdateResponse = UpdateResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed delete response
        // because the random delete response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.
        assertUpdateResponse(expectedUpdateResponse, parsedUpdateResponse);
    }

    public static void assertUpdateResponse(UpdateResponse expected, UpdateResponse actual) {
        IndexResponseTests.assertDocWriteResponse(expected, actual);
        assertEquals(expected.getGetResult(), actual.getGetResult());
    }

    /**
     * Returns a tuple of {@link UpdateResponse}s.
     * <p>
     * The left element is the actual {@link UpdateResponse} to serialize while the right element is the
     * expected {@link UpdateResponse} after parsing.
     */
    public static Tuple<UpdateResponse, UpdateResponse> randomUpdateResponse(XContentType xContentType) {
        Tuple<GetResult, GetResult> getResults = GetResultTests.randomGetResult(xContentType);
        GetResult actualGetResult = getResults.v1();
        GetResult expectedGetResult = getResults.v2();

        String index = actualGetResult.getIndex();
        String type = actualGetResult.getType();
        String id = actualGetResult.getId();
        long version = actualGetResult.getVersion();
        DocWriteResponse.Result result = actualGetResult.isExists() ? DocWriteResponse.Result.UPDATED : DocWriteResponse.Result.NOT_FOUND;
        String indexUUid = randomAsciiOfLength(5);
        int shardId = randomIntBetween(0, 5);

        // We also want small number values (randomNonNegativeLong() tend to generate high numbers)
        // in order to catch some conversion error that happen between int/long after parsing.
        Long seqNo = randomFrom(randomNonNegativeLong(), (long) randomIntBetween(0, 10_000), null);

        ShardId actualShardId = new ShardId(index, indexUUid, shardId);
        ShardId expectedShardId = new ShardId(index, INDEX_UUID_NA_VALUE, -1);

        UpdateResponse actual, expected;
        if (seqNo != null) {
            Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfos = RandomObjects.randomShardInfo(random());

            actual = new UpdateResponse(shardInfos.v1(), actualShardId, type, id, seqNo, version, result);
            expected = new UpdateResponse(shardInfos.v2(), expectedShardId, type, id, seqNo, version, result);
        } else {
            actual = new UpdateResponse(actualShardId, type, id, version, result);
            expected = new UpdateResponse(expectedShardId, type, id, version, result);
        }

        if (actualGetResult.isExists()) {
            actual.setGetResult(actualGetResult);
        }

        if (expectedGetResult.isExists()) {
            expected.setGetResult(expectedGetResult);
        }

        boolean forcedRefresh = randomBoolean();
        actual.setForcedRefresh(forcedRefresh);
        expected.setForcedRefresh(forcedRefresh);

        return Tuple.tuple(actual, expected);
    }
}
