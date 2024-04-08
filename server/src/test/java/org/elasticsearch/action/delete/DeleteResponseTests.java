/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.delete;

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
import java.util.function.Predicate;

import static org.elasticsearch.action.index.IndexResponseTests.assertDocWriteResponse;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class DeleteResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        {
            DeleteResponse response = new DeleteResponse(new ShardId("index", "index_uuid", 0), "id", 3, 17, 5, true);
            String output = Strings.toString(response);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 5,
                  "result": "deleted",
                  "_shards": null,
                  "_seq_no": 3,
                  "_primary_term": 17
                }"""), output);
        }
        {
            DeleteResponse response = new DeleteResponse(new ShardId("index", "index_uuid", 0), "id", -1, 0, 7, true);
            response.setForcedRefresh(true);
            response.setShardInfo(ReplicationResponse.ShardInfo.of(10, 5));
            String output = Strings.toString(response);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 7,
                  "result": "deleted",
                  "forced_refresh": true,
                  "_shards": {
                    "total": 10,
                    "successful": 5,
                    "failed": 0
                  }
                }
                """), output);
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
        final Tuple<DeleteResponse, DeleteResponse> tuple = randomDeleteResponse();
        DeleteResponse deleteResponse = tuple.v1();
        DeleteResponse expectedDeleteResponse = tuple.v2();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(deleteResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

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
        DeleteResponse parsedDeleteResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedDeleteResponse = parseInstance(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed delete response
        // because the random delete response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.
        assertDocWriteResponse(expectedDeleteResponse, parsedDeleteResponse);
    }

    private static DeleteResponse parseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        DeleteResponse.Builder context = new DeleteResponse.Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            BulkItemResponseTests.parseInnerToXContent(parser, context);
        }
        return context.build();
    }

    /**
     * Returns a tuple of {@link DeleteResponse}s.
     * <p>
     * The left element is the actual {@link DeleteResponse} to serialize while the right element is the
     * expected {@link DeleteResponse} after parsing.
     */
    public static Tuple<DeleteResponse, DeleteResponse> randomDeleteResponse() {
        String index = randomAlphaOfLength(5);
        String indexUUid = randomAlphaOfLength(5);
        int shardId = randomIntBetween(0, 5);
        String type = randomAlphaOfLength(5);
        String id = randomAlphaOfLength(5);
        long seqNo = randomFrom(SequenceNumbers.UNASSIGNED_SEQ_NO, randomNonNegativeLong(), (long) randomIntBetween(0, 10000));
        long primaryTerm = seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO ? 0 : randomIntBetween(1, 10000);
        long version = randomBoolean() ? randomNonNegativeLong() : randomIntBetween(0, 10000);
        boolean found = randomBoolean();
        boolean forcedRefresh = randomBoolean();

        Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfos = RandomObjects.randomShardInfo(random());

        DeleteResponse actual = new DeleteResponse(new ShardId(index, indexUUid, shardId), id, seqNo, primaryTerm, version, found);
        actual.setForcedRefresh(forcedRefresh);
        actual.setShardInfo(shardInfos.v1());

        DeleteResponse expected = new DeleteResponse(new ShardId(index, INDEX_UUID_NA_VALUE, -1), id, seqNo, primaryTerm, version, found);
        expected.setForcedRefresh(forcedRefresh);
        expected.setShardInfo(shardInfos.v2());

        return Tuple.tuple(actual, expected);
    }

}
