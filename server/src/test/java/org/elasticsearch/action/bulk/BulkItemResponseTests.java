/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.DeleteResponseTests;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.update.UpdateResponseTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.UUID;

import static org.elasticsearch.ElasticsearchExceptionTests.assertDeepEquals;
import static org.elasticsearch.ElasticsearchExceptionTests.randomExceptions;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.hamcrest.Matchers.containsString;

public class BulkItemResponseTests extends ESTestCase {

    public void testBulkItemResponseShouldContainTypeInV7CompatibilityMode() throws IOException {
        BulkItemResponse bulkItemResponse = BulkItemResponse.success(
            randomInt(),
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(
                new ShardId(randomAlphaOfLength(8), UUID.randomUUID().toString(), randomInt()),
                randomAlphaOfLength(4),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                true
            )
        );
        XContentBuilder xContentBuilder = bulkItemResponse.toXContent(
            XContentBuilder.builder(JsonXContent.jsonXContent, RestApiVersion.V_7),
            ToXContent.EMPTY_PARAMS
        );

        String json = BytesReference.bytes(xContentBuilder).utf8ToString();
        assertThat(json, containsString("\"_type\":\"_doc\""));
    }

    public void testFailureToString() {
        Failure failure = new Failure("index", "id", new RuntimeException("test"));
        String toString = failure.toString();
        assertThat(toString, containsString("\"type\":\"runtime_exception\""));
        assertThat(toString, containsString("\"reason\":\"test\""));
        assertThat(toString, containsString("\"status\":500"));
    }

    public void testToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        for (DocWriteRequest.OpType opType : DocWriteRequest.OpType.values()) {
            int bulkItemId = randomIntBetween(0, 100);
            boolean humanReadable = randomBoolean();

            Tuple<? extends DocWriteResponse, ? extends DocWriteResponse> randomDocWriteResponses = null;
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                randomDocWriteResponses = IndexResponseTests.randomIndexResponse();
            } else if (opType == DocWriteRequest.OpType.DELETE) {
                randomDocWriteResponses = DeleteResponseTests.randomDeleteResponse();
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                randomDocWriteResponses = UpdateResponseTests.randomUpdateResponse(xContentType);
            } else {
                fail("Test does not support opType [" + opType + "]");
            }

            BulkItemResponse bulkItemResponse = BulkItemResponse.success(bulkItemId, opType, randomDocWriteResponses.v1());
            BulkItemResponse expectedBulkItemResponse = BulkItemResponse.success(bulkItemId, opType, randomDocWriteResponses.v2());
            BytesReference originalBytes = toShuffledXContent(bulkItemResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

            BulkItemResponse parsedBulkItemResponse;
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                parsedBulkItemResponse = itemResponseFromXContent(parser, bulkItemId);
                assertNull(parser.nextToken());
            }
            assertBulkItemResponse(expectedBulkItemResponse, parsedBulkItemResponse);
        }
    }

    public void testFailureToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        int itemId = randomIntBetween(0, 100);
        String index = randomAlphaOfLength(5);
        String id = randomAlphaOfLength(5);
        DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

        final Tuple<Throwable, ElasticsearchException> exceptions = randomExceptions();

        Exception bulkItemCause = (Exception) exceptions.v1();
        Failure bulkItemFailure = new Failure(index, id, bulkItemCause);
        BulkItemResponse bulkItemResponse = BulkItemResponse.failure(itemId, opType, bulkItemFailure);
        Failure expectedBulkItemFailure = new Failure(index, id, exceptions.v2(), ExceptionsHelper.status(bulkItemCause));
        BulkItemResponse expectedBulkItemResponse = BulkItemResponse.failure(itemId, opType, expectedBulkItemFailure);
        BytesReference originalBytes = toShuffledXContent(bulkItemResponse, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                originalBytes = BytesReference.bytes(shuffleXContent(parser, randomBoolean()));
            }
        }

        BulkItemResponse parsedBulkItemResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedBulkItemResponse = itemResponseFromXContent(parser, itemId);
            assertNull(parser.nextToken());
        }
        assertBulkItemResponse(expectedBulkItemResponse, parsedBulkItemResponse);
    }

    public static void assertBulkItemResponse(BulkItemResponse expected, BulkItemResponse actual) {
        assertEquals(expected.getItemId(), actual.getItemId());
        assertEquals(expected.getIndex(), actual.getIndex());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getOpType(), actual.getOpType());
        assertEquals(expected.getVersion(), actual.getVersion());
        assertEquals(expected.isFailed(), actual.isFailed());

        if (expected.isFailed()) {
            BulkItemResponse.Failure expectedFailure = expected.getFailure();
            BulkItemResponse.Failure actualFailure = actual.getFailure();

            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getId(), actualFailure.getId());
            assertEquals(expectedFailure.getMessage(), actualFailure.getMessage());
            assertEquals(expectedFailure.getStatus(), actualFailure.getStatus());

            assertDeepEquals((ElasticsearchException) expectedFailure.getCause(), (ElasticsearchException) actualFailure.getCause());
        } else {
            DocWriteResponse expectedDocResponse = expected.getResponse();
            DocWriteResponse actualDocResponse = expected.getResponse();

            IndexResponseTests.assertDocWriteResponse(expectedDocResponse, actualDocResponse);
            if (expected.getOpType() == DocWriteRequest.OpType.UPDATE) {
                assertEquals(((UpdateResponse) expectedDocResponse).getGetResult(), ((UpdateResponse) actualDocResponse).getGetResult());
            }
        }
    }

    /**
     * Reads a {@link BulkItemResponse} from a {@link XContentParser}.
     *
     * @param parser the {@link XContentParser}
     * @param id the id to assign to the parsed {@link BulkItemResponse}. It is usually the index of
     *           the item in the {@link BulkResponse#getItems} array.
     */
    public static BulkItemResponse itemResponseFromXContent(XContentParser parser, int id) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        final DocWriteRequest.OpType opType = DocWriteRequest.OpType.fromString(currentFieldName);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        DocWriteResponse.Builder builder = null;
        CheckedConsumer<XContentParser, IOException> itemParser = null;

        if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
            final IndexResponse.Builder indexResponseBuilder = new IndexResponse.Builder();
            builder = indexResponseBuilder;
            itemParser = (indexParser) -> IndexResponse.parseXContentFields(indexParser, indexResponseBuilder);

        } else if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateResponse.Builder updateResponseBuilder = new UpdateResponse.Builder();
            builder = updateResponseBuilder;
            itemParser = (updateParser) -> UpdateResponse.parseXContentFields(updateParser, updateResponseBuilder);

        } else if (opType == DocWriteRequest.OpType.DELETE) {
            final DeleteResponse.Builder deleteResponseBuilder = new DeleteResponse.Builder();
            builder = deleteResponseBuilder;
            itemParser = (deleteParser) -> DeleteResponse.parseXContentFields(deleteParser, deleteResponseBuilder);
        } else {
            throwUnknownField(currentFieldName, parser);
        }

        RestStatus status = null;
        ElasticsearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }

            if (BulkItemResponse.ERROR.equals(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    exception = ElasticsearchException.fromXContent(parser);
                }
            } else if (BulkItemResponse.STATUS.equals(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                itemParser.accept(parser);
            }
        }

        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);

        BulkItemResponse bulkItemResponse;
        if (exception != null) {
            Failure failure = new Failure(builder.getShardId().getIndexName(), builder.getId(), exception, status);
            bulkItemResponse = BulkItemResponse.failure(id, opType, failure);
        } else {
            bulkItemResponse = BulkItemResponse.success(id, opType, builder.build());
        }
        return bulkItemResponse;
    }
}
