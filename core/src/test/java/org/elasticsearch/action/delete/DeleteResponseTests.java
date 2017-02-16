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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.index.IndexResponseTests.assertDocWriteResponse;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class DeleteResponseTests extends ESTestCase {

    public void testToXContent() {
        {
            DeleteResponse response = new DeleteResponse(new ShardId("index", "index_uuid", 0), "type", "id", 3, 5, true);
            String output = Strings.toString(response);
            assertEquals("{\"found\":true,\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":5,\"result\":\"deleted\"," +
                "\"_shards\":null,\"_seq_no\":3}", output);
        }
        {
            DeleteResponse response = new DeleteResponse(new ShardId("index", "index_uuid", 0), "type", "id", -1, 7, true);
            response.setForcedRefresh(true);
            response.setShardInfo(new ReplicationResponse.ShardInfo(10, 5));
            String output = Strings.toString(response);
            assertEquals("{\"found\":true,\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":7,\"result\":\"deleted\"," +
                "\"forced_refresh\":true,\"_shards\":{\"total\":10,\"successful\":5,\"failed\":0}}", output);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        // Create a random DeleteResponse and converts it to XContent in bytes
        DeleteResponse deleteResponse = randomDeleteResponse();
        boolean humanReadable = randomBoolean();
        BytesReference deleteResponseBytes = toXContent(deleteResponse, xContentType, humanReadable);

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), deleteResponseBytes)) {
                deleteResponseBytes = shuffleXContent(parser, randomBoolean()).bytes();
            }
        }

        // Parse the XContent bytes to obtain a parsed
        DeleteResponse parsedDeleteResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), deleteResponseBytes)) {
            parsedDeleteResponse = DeleteResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed delete response
        // because the random delete response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.

        // Print the parsed object out and test that the output is the same as the original output
        BytesReference parsedDeleteResponseBytes = toXContent(parsedDeleteResponse, xContentType, humanReadable);
        try (XContentParser parser = createParser(xContentType.xContent(), parsedDeleteResponseBytes)) {
            assertDeleteResponse(deleteResponse, parser.map());
        }
    }

    public static void assertDeleteResponse(DeleteResponse expected, Map<String, Object> actual) {
        assertDocWriteResponse(expected, actual);
        if (expected.getResult() == DocWriteResponse.Result.DELETED) {
            assertTrue((boolean) actual.get("found"));
        } else {
            assertFalse((boolean) actual.get("found"));
        }
    }

    public static DeleteResponse randomDeleteResponse() {
        ShardId shardId = new ShardId(randomAsciiOfLength(5), randomAsciiOfLength(5), randomIntBetween(0, 5));
        String type = randomAsciiOfLength(5);
        String id = randomAsciiOfLength(5);
        long seqNo = randomFrom(SequenceNumbersService.UNASSIGNED_SEQ_NO, randomNonNegativeLong(), (long) randomIntBetween(0, 10000));
        long version = randomBoolean() ? randomNonNegativeLong() : randomIntBetween(0, 10000);
        boolean found = randomBoolean();

        DeleteResponse response = new DeleteResponse(shardId, type, id, seqNo, version, found);
        response.setForcedRefresh(randomBoolean());
        response.setShardInfo(RandomObjects.randomShardInfo(random(), randomBoolean()));
        return response;
    }

}
