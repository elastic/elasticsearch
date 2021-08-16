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
import org.elasticsearch.action.delete.DeleteResponseTests;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.update.UpdateResponseTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.ElasticsearchExceptionTests.randomExceptions;
import static org.elasticsearch.action.bulk.BulkItemResponseTests.assertBulkItemResponse;
import static org.elasticsearch.action.bulk.BulkResponse.NO_INGEST_TOOK;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

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

                bulkItems[i] = BulkItemResponse.success(i, opType, randomDocWriteResponses.v1());
                expectedBulkItems[i] = BulkItemResponse.success(i, opType, randomDocWriteResponses.v2());
            } else {
                String index = randomAlphaOfLength(5);
                String type = randomAlphaOfLength(5);
                String id = randomAlphaOfLength(5);

                Tuple<Throwable, ElasticsearchException> failures = randomExceptions();

                Exception bulkItemCause = (Exception) failures.v1();
                bulkItems[i] = BulkItemResponse.failure(i, opType,
                        new BulkItemResponse.Failure(index, type, id, bulkItemCause));
                expectedBulkItems[i] = BulkItemResponse.failure(i, opType,
                        new BulkItemResponse.Failure(index, type, id, failures.v2(), ExceptionsHelper.status(bulkItemCause)));
            }
        }

        BulkResponse bulkResponse = new BulkResponse(bulkItems, took, ingestTook);
        BytesReference originalBytes = toShuffledXContent(bulkResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BulkResponse parsedBulkResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedBulkResponse = BulkResponse.fromXContent(parser);
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
}
