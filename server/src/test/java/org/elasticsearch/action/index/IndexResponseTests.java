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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.action.support.replication.ReplicationResponseTests.assertShardInfo;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class IndexResponseTests extends ESTestCase {

    public void testToXContent() {
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "id", 3, 17, 5, true);
            String output = Strings.toString(indexResponse);
            assertEquals("{\"_index\":\"index\",\"_id\":\"id\",\"_version\":5,\"result\":\"created\",\"_shards\":null," +
                    "\"_seq_no\":3,\"_primary_term\":17}", output);
        }
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "id", -1, 17, 7, true);
            indexResponse.setForcedRefresh(true);
            indexResponse.setShardInfo(new ReplicationResponse.ShardInfo(10, 5));
            String output = Strings.toString(indexResponse);
            assertEquals("{\"_index\":\"index\",\"_id\":\"id\",\"_version\":7,\"result\":\"created\"," +
                    "\"forced_refresh\":true,\"_shards\":{\"total\":10,\"successful\":5,\"failed\":0}}", output);
        }
    }

    public void testToAndFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final Tuple<IndexResponse, IndexResponse> tuple = randomIndexResponse();
        IndexResponse indexResponse = tuple.v1();
        IndexResponse expectedIndexResponse = tuple.v2();

        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(indexResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BytesReference mutated;
        if (addRandomFields) {
            // The ShardInfo.Failure's exception is rendered out in a "reason" object. We shouldn't add anything random there
            // because exception rendering and parsing are very permissive: any extra object or field would be rendered as
            // a exception custom metadata and be parsed back as a custom header, making it impossible to compare the results
            // in this test.
            Predicate<String> excludeFilter = path -> path.contains("reason");
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        IndexResponse parsedIndexResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
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
        String index = randomAlphaOfLength(5);
        String indexUUid = randomAlphaOfLength(5);
        int shardId = randomIntBetween(0, 5);
        String id = randomAlphaOfLength(5);
        long seqNo = randomFrom(SequenceNumbers.UNASSIGNED_SEQ_NO, randomNonNegativeLong(), (long) randomIntBetween(0, 10000));
        long primaryTerm = seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO ? 0 : randomIntBetween(1, 10000);
        long version = randomBoolean() ? randomNonNegativeLong() : randomIntBetween(0, 10000);
        boolean created = randomBoolean();
        boolean forcedRefresh = randomBoolean();

        Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfos = RandomObjects.randomShardInfo(random());

        IndexResponse actual = new IndexResponse(new ShardId(index, indexUUid, shardId), id, seqNo, primaryTerm, version, created);
        actual.setForcedRefresh(forcedRefresh);
        actual.setShardInfo(shardInfos.v1());

        IndexResponse expected =
                new IndexResponse(new ShardId(index, INDEX_UUID_NA_VALUE, -1), id, seqNo, primaryTerm, version, created);
        expected.setForcedRefresh(forcedRefresh);
        expected.setShardInfo(shardInfos.v2());

        return Tuple.tuple(actual, expected);
    }
}
