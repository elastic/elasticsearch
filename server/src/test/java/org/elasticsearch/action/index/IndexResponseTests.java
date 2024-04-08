/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponseTests;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.action.support.replication.ReplicationResponseTests.assertShardInfo;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class IndexResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "id", 3, 17, 5, true);
            String output = Strings.toString(indexResponse);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 5,
                  "result": "created",
                  "_shards": null,
                  "_seq_no": 3,
                  "_primary_term": 17
                }"""), output);
        }
        {
            IndexResponse indexResponse = new IndexResponse(new ShardId("index", "index_uuid", 0), "id", -1, 17, 7, true);
            indexResponse.setForcedRefresh(true);
            indexResponse.setShardInfo(ReplicationResponse.ShardInfo.of(10, 5));
            String output = Strings.toString(indexResponse);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 7,
                  "result": "created",
                  "forced_refresh": true,
                  "_shards": {
                    "total": 10,
                    "successful": 5,
                    "failed": 0
                  }
                }"""), output);
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

    public void testSerialization() throws IOException {
        // Note: IndexRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a Writable
        Tuple<IndexResponse, IndexResponse> responseTuple = randomIndexResponse();
        IndexResponse copy = copyWriteable(responseTuple.v1(), null, IndexResponse::new);
        assertDocWriteResponse(responseTuple.v1(), copy);
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
            parsedIndexResponse = parseInstanceFromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed index response
        // because the random index response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.
        assertDocWriteResponse(expectedIndexResponse, parsedIndexResponse);
    }

    private static IndexResponse parseInstanceFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        IndexResponse.Builder context = new IndexResponse.Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            BulkItemResponseTests.parseInnerToXContent(parser, context);
        }
        return context.build();
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
        boolean includePipelines = randomBoolean();
        final List<String> pipelines;
        if (includePipelines) {
            pipelines = new ArrayList<>();
            for (int i = 0; i < randomIntBetween(0, 20); i++) {
                pipelines.add(randomAlphaOfLength(20));
            }
        } else {
            pipelines = null;
        }
        IndexResponse actual = new IndexResponse(
            new ShardId(index, indexUUid, shardId),
            id,
            seqNo,
            primaryTerm,
            version,
            created,
            pipelines
        );
        actual.setForcedRefresh(forcedRefresh);
        actual.setShardInfo(shardInfos.v1());

        IndexResponse expected = new IndexResponse(
            new ShardId(index, INDEX_UUID_NA_VALUE, -1),
            id,
            seqNo,
            primaryTerm,
            version,
            created,
            pipelines
        );
        expected.setForcedRefresh(forcedRefresh);
        expected.setShardInfo(shardInfos.v2());

        return Tuple.tuple(actual, expected);
    }
}
