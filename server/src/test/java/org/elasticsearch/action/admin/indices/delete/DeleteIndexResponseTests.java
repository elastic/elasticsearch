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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class DeleteIndexResponseTests extends ESTestCase {

    public void testToXContent() {
        DeleteIndexResponse response = new DeleteIndexResponse(true);
        String output = Strings.toString(response);
        assertEquals("{\"acknowledged\":true}", output);
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

        final DeleteIndexResponse deleteIndexResponse = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(deleteIndexResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        DeleteIndexResponse parsedDeleteIndexResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedDeleteIndexResponse = DeleteIndexResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(deleteIndexResponse.isAcknowledged(), parsedDeleteIndexResponse.isAcknowledged());
    }

    /**
     * Returns a random {@link DeleteIndexResponse}.
     */
    private static DeleteIndexResponse createTestItem() throws IOException {
        boolean acknowledged = randomBoolean();

        return new DeleteIndexResponse(acknowledged);
    }
}
