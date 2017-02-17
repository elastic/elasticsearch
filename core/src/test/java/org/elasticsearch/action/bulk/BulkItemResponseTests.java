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

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.delete.DeleteResponseTests;
import org.elasticsearch.action.index.IndexResponseTests;
import org.elasticsearch.action.update.UpdateResponseTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.ElasticsearchExceptionTests.assertDeepEquals;
import static org.elasticsearch.ElasticsearchExceptionTests.randomExceptions;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.containsString;

public class BulkItemResponseTests extends ESTestCase {

    public void testFailureToString() {
        Failure failure = new Failure("index", "type", "id", new RuntimeException("test"));
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

            BulkItemResponse bulkItemResponse = new BulkItemResponse(bulkItemId, opType, randomDocWriteResponses.v1());
            BulkItemResponse expectedBulkItemResponse = new BulkItemResponse(bulkItemId, opType, randomDocWriteResponses.v2());
            BytesReference originalBytes = toXContent(bulkItemResponse, xContentType, humanReadable);

            // Shuffle the XContent fields
            if (randomBoolean()) {
                try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                    originalBytes = shuffleXContent(parser, randomBoolean()).bytes();
                }
            }

            BulkItemResponse parsedBulkItemResponse;
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                parsedBulkItemResponse = BulkItemResponse.fromXContent(parser, bulkItemId);
                assertNull(parser.nextToken());
            }

            assertEquals(expectedBulkItemResponse.getIndex(), parsedBulkItemResponse.getIndex());
            assertEquals(expectedBulkItemResponse.getType(), parsedBulkItemResponse.getType());
            assertEquals(expectedBulkItemResponse.getId(), parsedBulkItemResponse.getId());
            assertEquals(expectedBulkItemResponse.getOpType(), parsedBulkItemResponse.getOpType());
            assertEquals(expectedBulkItemResponse.getVersion(), parsedBulkItemResponse.getVersion());
            assertEquals(bulkItemId, parsedBulkItemResponse.getItemId());

            if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateResponseTests.assertUpdateResponse(expectedBulkItemResponse.getResponse(), parsedBulkItemResponse.getResponse());
            } else {
                IndexResponseTests.assertDocWriteResponse(expectedBulkItemResponse.getResponse(), parsedBulkItemResponse.getResponse());
            }
        }
    }

    public void testFailureToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final Tuple<Throwable, ElasticsearchException> exceptions = randomExceptions();
        final Throwable cause = exceptions.v1();
        final ElasticsearchException expectedCause = exceptions.v2();

        int bulkItemId = randomIntBetween(0, 100);
        String index = randomAsciiOfLength(5);
        String type = randomAsciiOfLength(5);
        String id = randomAsciiOfLength(5);
        DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

        BulkItemResponse bulkItemResponse = new BulkItemResponse(bulkItemId, opType, new Failure(index, type, id, (Exception) cause));
        BytesReference originalBytes = toXContent(bulkItemResponse, xContentType, randomBoolean());

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                originalBytes = shuffleXContent(parser, randomBoolean()).bytes();
            }
        }

        BulkItemResponse parsedBulkItemResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedBulkItemResponse = BulkItemResponse.fromXContent(parser, bulkItemId);
            assertNull(parser.nextToken());
        }

        assertNotNull(parsedBulkItemResponse);
        assertEquals(index, parsedBulkItemResponse.getIndex());
        assertEquals(type, parsedBulkItemResponse.getType());
        assertEquals(id, parsedBulkItemResponse.getId());
        assertEquals(opType, parsedBulkItemResponse.getOpType());
        assertEquals(bulkItemId, parsedBulkItemResponse.getItemId());

        Failure parsedFailure = parsedBulkItemResponse.getFailure();
        assertEquals(index, parsedFailure.getIndex());
        assertEquals(type, parsedFailure.getType());
        assertEquals(id, parsedFailure.getId());

        assertDeepEquals(expectedCause, (ElasticsearchException) parsedFailure.getCause());
    }
}
