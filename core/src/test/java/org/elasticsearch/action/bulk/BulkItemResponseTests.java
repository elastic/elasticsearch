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

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.delete.DeleteResponseTests.assertDeleteResponse;
import static org.elasticsearch.action.delete.DeleteResponseTests.randomDeleteResponse;
import static org.elasticsearch.action.index.IndexResponseTests.assertIndexResponse;
import static org.elasticsearch.action.index.IndexResponseTests.randomIndexResponse;
import static org.elasticsearch.action.update.UpdateResponseTests.assertUpdateResponse;
import static org.elasticsearch.action.update.UpdateResponseTests.randomUpdateResponse;
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
            int id = randomIntBetween(0, 100);
            boolean humanReadable = randomBoolean();
            BytesReference originalBytes = null;
            BulkItemResponse expectedBulkItemResponse = null;

            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                expectedBulkItemResponse = new BulkItemResponse(id, opType, randomIndexResponse());
                originalBytes = toXContent(expectedBulkItemResponse, xContentType, humanReadable);

            } else if (opType == DocWriteRequest.OpType.DELETE) {
                expectedBulkItemResponse = new BulkItemResponse(id, opType, randomDeleteResponse());
                originalBytes = toXContent(expectedBulkItemResponse, xContentType, humanReadable);

            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                Tuple<UpdateResponse, UpdateResponse> updates = randomUpdateResponse(xContentType);
                expectedBulkItemResponse = new BulkItemResponse(id, opType, updates.v2());
                originalBytes = toXContent(new BulkItemResponse(id, opType, updates.v1()), xContentType, humanReadable);

            } else {
                fail("Test does not support opType [" + opType + "]");
            }

            // Shuffle the XContent fields
            if (randomBoolean()) {
                try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                    originalBytes = shuffleXContent(parser, randomBoolean()).bytes();
                }
            }

            BulkItemResponse parsedBulkItemResponse;
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                parsedBulkItemResponse = BulkItemResponse.fromXContent(parser);
                assertNull(parser.nextToken());
            }

            assertEquals(expectedBulkItemResponse.getIndex(), parsedBulkItemResponse.getIndex());
            assertEquals(expectedBulkItemResponse.getType(), parsedBulkItemResponse.getType());
            assertEquals(expectedBulkItemResponse.getId(), parsedBulkItemResponse.getId());
            assertEquals(expectedBulkItemResponse.getOpType(), parsedBulkItemResponse.getOpType());
            assertEquals(expectedBulkItemResponse.getVersion(), parsedBulkItemResponse.getVersion());

            BytesReference finalBytes = toXContent(parsedBulkItemResponse, xContentType, humanReadable);
            try (XContentParser parser = createParser(xContentType.xContent(), finalBytes)) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals(opType.getLowercase(), parser.currentName());
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());

                Map<String, Object> map = parser.map();

                if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                    assertIndexResponse(expectedBulkItemResponse.getResponse(), map);
                } else if (opType == DocWriteRequest.OpType.DELETE) {
                    assertDeleteResponse(expectedBulkItemResponse.getResponse(), map);
                } else if (opType == DocWriteRequest.OpType.UPDATE) {
                    assertUpdateResponse(expectedBulkItemResponse.getResponse(), parsedBulkItemResponse.getResponse(), map);
                } else {
                    fail("Test does not support opType [" + opType + "]");
                }
            }
        }
    }
}
