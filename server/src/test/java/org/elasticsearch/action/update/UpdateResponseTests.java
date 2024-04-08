/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponseTests;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.action.DocWriteResponse.Result.DELETED;
import static org.elasticsearch.action.DocWriteResponse.Result.NOT_FOUND;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class UpdateResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        {
            UpdateResponse updateResponse = new UpdateResponse(new ShardId("index", "index_uuid", 0), "id", -2, 0, 0, NOT_FOUND);
            String output = Strings.toString(updateResponse);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 0,
                  "result": "not_found",
                  "_shards": {
                    "total": 0,
                    "successful": 0,
                    "failed": 0
                  }
                }"""), output);
        }
        {
            UpdateResponse updateResponse = new UpdateResponse(
                ReplicationResponse.ShardInfo.of(10, 6),
                new ShardId("index", "index_uuid", 1),
                "id",
                3,
                17,
                1,
                DELETED
            );
            String output = Strings.toString(updateResponse);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "index",
                  "_id": "id",
                  "_version": 1,
                  "result": "deleted",
                  "_shards": {
                    "total": 10,
                    "successful": 6,
                    "failed": 0
                  },
                  "_seq_no": 3,
                  "_primary_term": 17
                }"""), output);
        }
        {
            BytesReference source = new BytesArray("""
                {"title":"Book title","isbn":"ABC-123"}""");
            Map<String, DocumentField> fields = new HashMap<>();
            fields.put("title", new DocumentField("title", Collections.singletonList("Book title")));
            fields.put("isbn", new DocumentField("isbn", Collections.singletonList("ABC-123")));

            UpdateResponse updateResponse = new UpdateResponse(
                ReplicationResponse.ShardInfo.of(3, 2),
                new ShardId("books", "books_uuid", 2),
                "1",
                7,
                17,
                2,
                UPDATED
            );
            updateResponse.setGetResult(new GetResult("books", "1", 0, 1, 2, true, source, fields, null));

            String output = Strings.toString(updateResponse);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_index": "books",
                  "_id": "1",
                  "_version": 2,
                  "result": "updated",
                  "_shards": {
                    "total": 3,
                    "successful": 2,
                    "failed": 0
                  },
                  "_seq_no": 7,
                  "_primary_term": 17,
                  "get": {
                    "_seq_no": 0,
                    "_primary_term": 1,
                    "found": true,
                    "_source": {
                      "title": "Book title",
                      "isbn": "ABC-123"
                    },
                    "fields": {
                      "isbn": [ "ABC-123" ],
                      "title": [ "Book title" ]
                    }
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

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.JSON);
        final Tuple<UpdateResponse, UpdateResponse> tuple = randomUpdateResponse(xContentType);
        UpdateResponse updateResponse = tuple.v1();
        UpdateResponse expectedUpdateResponse = tuple.v2();

        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(updateResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BytesReference mutated;
        if (addRandomFields) {
            // - The ShardInfo.Failure's exception is rendered out in a "reason" object. We shouldn't add anything random there
            // because exception rendering and parsing are very permissive: any extra object or field would be rendered as
            // a exception custom metadata and be parsed back as a custom header, making it impossible to compare the results
            // in this test.
            // - The GetResult's "_source" and "fields" just consists of key/value pairs, we shouldn't add anything random there.
            // It is already randomized in the randomGetResult() method anyway. Also, we cannot add anything within the "get"
            // object since this is where GetResult's metadata fields are rendered out and they would be parsed back as
            // extra metadata fields.
            Predicate<String> excludeFilter = path -> path.contains("reason") || path.contains("get");
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        UpdateResponse parsedUpdateResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedUpdateResponse = parseInstanceFromXContent(parser);
            assertNull(parser.nextToken());
        }

        IndexResponseTests.assertDocWriteResponse(expectedUpdateResponse, parsedUpdateResponse);
        if (addRandomFields == false) {
            assertEquals(expectedUpdateResponse.getGetResult(), parsedUpdateResponse.getGetResult());
        }

        // Prints out the parsed UpdateResponse object to verify that it is the same as the expected output.
        // If random fields have been inserted, it checks that they have been filtered out and that they do
        // not alter the final output of the parsed object.
        BytesReference parsedBytes = toXContent(parsedUpdateResponse, xContentType, humanReadable);
        BytesReference expectedBytes = toXContent(expectedUpdateResponse, xContentType, humanReadable);
        assertToXContentEquivalent(expectedBytes, parsedBytes, xContentType);
    }

    private static UpdateResponse parseInstanceFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        UpdateResponse.Builder context = new UpdateResponse.Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, UpdateResponse.Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();

        if (UpdateResponse.GET.equals(currentFieldName)) {
            if (token == XContentParser.Token.START_OBJECT) {
                context.setGetResult(GetResult.fromXContentEmbedded(parser));
            }
        } else {
            BulkItemResponseTests.parseInnerToXContent(parser, context);
        }
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
        String id = actualGetResult.getId();
        long version = actualGetResult.getVersion();
        DocWriteResponse.Result result = actualGetResult.isExists() ? DocWriteResponse.Result.UPDATED : DocWriteResponse.Result.NOT_FOUND;
        String indexUUid = randomAlphaOfLength(5);
        int shardId = randomIntBetween(0, 5);

        // We also want small number values (randomNonNegativeLong() tend to generate high numbers)
        // in order to catch some conversion error that happen between int/long after parsing.
        long seqNo = randomFrom(randomNonNegativeLong(), (long) randomIntBetween(0, 10_000), SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO ? SequenceNumbers.UNASSIGNED_PRIMARY_TERM : randomIntBetween(1, 16);

        ShardId actualShardId = new ShardId(index, indexUUid, shardId);
        ShardId expectedShardId = new ShardId(index, INDEX_UUID_NA_VALUE, -1);

        UpdateResponse actual, expected;
        if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfos = RandomObjects.randomShardInfo(random());

            actual = new UpdateResponse(shardInfos.v1(), actualShardId, id, seqNo, primaryTerm, version, result);
            expected = new UpdateResponse(shardInfos.v2(), expectedShardId, id, seqNo, primaryTerm, version, result);
        } else {
            actual = new UpdateResponse(actualShardId, id, seqNo, primaryTerm, version, result);
            expected = new UpdateResponse(expectedShardId, id, seqNo, primaryTerm, version, result);
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
