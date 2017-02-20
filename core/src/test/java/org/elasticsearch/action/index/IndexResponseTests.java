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

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;

import static org.elasticsearch.action.support.replication.ReplicationResponseTests.assertShardInfo;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class IndexResponseTests extends ESTestCase {

    public void testToXContent() {
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "type", "id", 3, 5, true);
            String output = Strings.toString(indexResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":5,\"result\":\"created\",\"_shards\":null," +
                    "\"_seq_no\":3,\"created\":true}", output);
        }
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "type", "id", -1, 7, true);
            indexResponse.setForcedRefresh(true);
            indexResponse.setShardInfo(new ReplicationResponse.ShardInfo(10, 5));
            String output = Strings.toString(indexResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":7,\"result\":\"created\"," +
                    "\"forced_refresh\":true,\"_shards\":{\"total\":10,\"successful\":5,\"failed\":0},\"created\":true}", output);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final Tuple<IndexResponse, IndexResponse> tuple = randomIndexResponse();
        IndexResponse indexResponse = tuple.v1();
        IndexResponse expectedIndexResponse = tuple.v2();

        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference indexResponseBytes = toXContent(indexResponse, xContentType, humanReadable);

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), indexResponseBytes)) {
                indexResponseBytes = shuffleXContent(parser, randomBoolean()).bytes();
            }
        }

        // Parse the XContent bytes to obtain a parsed
        IndexResponse parsedIndexResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), indexResponseBytes)) {
            parsedIndexResponse = IndexResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed index response
        // because the random index response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.
        assertDocWriteResponse(expectedIndexResponse, parsedIndexResponse);
    }

    public static void assertDocWriteResponse(DocWriteResponse expected, DocWriteResponse actual) {
        assertEquals(expected.getIndex(), actual.getIndex());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getSeqNo(), actual.getSeqNo());
        assertEquals(expected.getResult(), actual.getResult());
        assertEquals(expected.getShardId(), actual.getShardId());
        assertEquals(expected.forcedRefresh(), actual.forcedRefresh());
        assertEquals(expected.status(), actual.status());
        assertShardInfo(expected.getShardInfo(), actual.getShardInfo());
    }

    /**
     * Returns a tuple of {@link IndexResponse}s.
     * <p>
     * The left element is the actual {@link IndexResponse} to serialize while the right element is the
     * expected {@link IndexResponse} after parsing.
     */
    public static Tuple<IndexResponse, IndexResponse> randomIndexResponse() {
        String index = randomAsciiOfLength(5);
        String indexUUid = randomAsciiOfLength(5);
        int shardId = randomIntBetween(0, 5);
        String type = randomAsciiOfLength(5);
        String id = randomAsciiOfLength(5);
        long seqNo = randomFrom(SequenceNumbersService.UNASSIGNED_SEQ_NO, randomNonNegativeLong(), (long) randomIntBetween(0, 10000));
        long version = randomBoolean() ? randomNonNegativeLong() : randomIntBetween(0, 10000);
        boolean created = randomBoolean();
        boolean forcedRefresh = randomBoolean();

        Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfos = RandomObjects.randomShardInfo(random());

        IndexResponse actual = new IndexResponse(new ShardId(index, indexUUid, shardId), type, id, seqNo, version, created);
        actual.setForcedRefresh(forcedRefresh);
        actual.setShardInfo(shardInfos.v1());

        IndexResponse expected = new IndexResponse(new ShardId(index, INDEX_UUID_NA_VALUE, -1), type, id, seqNo, version, created);
        expected.setForcedRefresh(forcedRefresh);
        expected.setShardInfo(shardInfos.v2());

        return Tuple.tuple(actual, expected);
    }
}
