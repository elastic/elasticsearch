/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponseTests;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.update.UpdateResponseTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.bulk.BulkItemResponseTests.assertBulkItemResponse;
import static org.elasticsearch.action.bulk.BulkResponse.NO_INGEST_TOOK;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.exception.ElasticsearchExceptionTests.randomExceptions;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;

public class BulkResponseTests extends ESTestCase {

    public void testToAndFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();

        long took = randomFrom(randomNonNegativeLong(), -1L);
        long ingestTook = randomFrom(randomNonNegativeLong(), NO_INGEST_TOOK);
        int nbBulkItems = randomIntBetween(1, 10);

        BulkItemResponse[] bulkItems = new BulkItemResponse[nbBulkItems];
        BulkItemResponse[] expectedBulkItems = new BulkItemResponse[nbBulkItems];

        for (int i = 0; i < nbBulkItems; i++) {
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            if (frequently()) {
                Tuple<? extends DocWriteResponse, ? extends DocWriteResponse> randomDocWriteResponses = success(opType, xContentType);
                bulkItems[i] = BulkItemResponse.success(i, opType, randomDocWriteResponses.v1());
                expectedBulkItems[i] = BulkItemResponse.success(i, opType, randomDocWriteResponses.v2());
            } else {
                String index = randomAlphaOfLength(5);
                String id = randomAlphaOfLength(5);

                Tuple<Throwable, ElasticsearchException> failures = randomExceptions();

                Exception bulkItemCause = (Exception) failures.v1();
                bulkItems[i] = BulkItemResponse.failure(i, opType, new BulkItemResponse.Failure(index, id, bulkItemCause));
                expectedBulkItems[i] = BulkItemResponse.failure(
                    i,
                    opType,
                    new BulkItemResponse.Failure(index, id, failures.v2(), ExceptionsHelper.status(bulkItemCause))
                );
            }
        }

        BulkResponse bulkResponse = new BulkResponse(bulkItems, took, ingestTook);
        BytesReference originalBytes = toShuffledXContent(
            ChunkedToXContent.wrapAsToXContent(bulkResponse),
            xContentType,
            ToXContent.EMPTY_PARAMS,
            humanReadable
        );

        BulkResponse parsedBulkResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedBulkResponse = fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(took, parsedBulkResponse.getTook().getMillis());
        assertEquals(ingestTook, parsedBulkResponse.getIngestTookInMillis());
        assertEquals(expectedBulkItems.length, parsedBulkResponse.getItems().length);

        for (int i = 0; i < expectedBulkItems.length; i++) {
            assertBulkItemResponse(expectedBulkItems[i], parsedBulkResponse.getItems()[i]);
        }

        BytesReference finalBytes = toXContent(parsedBulkResponse, xContentType, humanReadable);
        BytesReference expectedFinalBytes = toXContent(parsedBulkResponse, xContentType, humanReadable);
        assertToXContentEquivalent(expectedFinalBytes, finalBytes, xContentType);
    }

    public void testToXContentPlacesErrorsFirst() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();

        boolean errors = false;
        long took = randomFrom(randomNonNegativeLong(), -1L);
        long ingestTook = randomFrom(randomNonNegativeLong(), NO_INGEST_TOOK);
        int nbBulkItems = randomIntBetween(1, 10);

        BulkItemResponse[] bulkItems = new BulkItemResponse[nbBulkItems];

        for (int i = 0; i < nbBulkItems; i++) {
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());
            if (frequently()) {
                Tuple<? extends DocWriteResponse, ? extends DocWriteResponse> randomDocWriteResponses = success(opType, xContentType);

                BulkItemResponse success = BulkItemResponse.success(i, opType, randomDocWriteResponses.v1());
                bulkItems[i] = success;
            } else {
                errors = true;
                String index = randomAlphaOfLength(5);
                String id = randomAlphaOfLength(5);

                Tuple<Throwable, ElasticsearchException> failures = randomExceptions();

                Exception bulkItemCause = (Exception) failures.v1();
                bulkItems[i] = BulkItemResponse.failure(i, opType, new BulkItemResponse.Failure(index, id, bulkItemCause));
            }
        }

        BulkResponse bulkResponse = new BulkResponse(bulkItems, took, ingestTook);
        BytesReference originalBytes = toXContent(bulkResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            XContentParser.Token firstField = parser.nextToken();
            assertThat(firstField, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("errors"));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_BOOLEAN));
            assertThat(parser.booleanValue(), equalTo(errors));
        }
    }

    private static Tuple<? extends DocWriteResponse, ? extends DocWriteResponse> success(
        DocWriteRequest.OpType opType,
        XContentType xContentType
    ) {
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
        return randomDocWriteResponses;
    }

    private static BulkResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        long took = -1L;
        long ingestTook = NO_INGEST_TOOK;
        List<BulkItemResponse> items = new ArrayList<>();

        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (BulkResponse.TOOK.equals(currentFieldName)) {
                    took = parser.longValue();
                } else if (BulkResponse.INGEST_TOOK.equals(currentFieldName)) {
                    ingestTook = parser.longValue();
                } else if (BulkResponse.ERRORS.equals(currentFieldName) == false) {
                    throwUnknownField(currentFieldName, parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BulkResponse.ITEMS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        items.add(BulkItemResponseTests.itemResponseFromXContent(parser, items.size()));
                    }
                } else {
                    throwUnknownField(currentFieldName, parser);
                }
            } else {
                throwUnknownToken(token, parser);
            }
        }
        return new BulkResponse(items.toArray(new BulkItemResponse[items.size()]), took, ingestTook);
    }
}
