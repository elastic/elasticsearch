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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class CreateIndexResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        CreateIndexResponse response = new CreateIndexResponse(true, true, "foo");

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                CreateIndexResponse serialized = new CreateIndexResponse();
                serialized.readFrom(in);
                assertEquals(response.isShardsAcked(), serialized.isShardsAcked());
                assertEquals(response.isAcknowledged(), serialized.isAcknowledged());
                assertEquals(response.index(), serialized.index());
            }
        }
    }

    public void testSerializationWithOldVersion() throws IOException {
        Version oldVersion = Version.V_5_4_0;
        CreateIndexResponse response = new CreateIndexResponse(true, true, "foo");

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(oldVersion);
            response.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(oldVersion);
                CreateIndexResponse serialized = new CreateIndexResponse();
                serialized.readFrom(in);
                assertEquals(response.isShardsAcked(), serialized.isShardsAcked());
                assertEquals(response.isAcknowledged(), serialized.isAcknowledged());
                assertNull(serialized.index());
            }
        }
    }

    public void testToXContent() {
        CreateIndexResponse response = new CreateIndexResponse(true, false, "index_name");
        String output = Strings.toString(response);
        assertEquals("{\"acknowledged\":true,\"shards_acknowledged\":false,\"index\":\"index_name\"}", output);
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

        final CreateIndexResponse createIndexResponse = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(createIndexResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        CreateIndexResponse parsedCreateIndexResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedCreateIndexResponse = CreateIndexResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(createIndexResponse.index(), parsedCreateIndexResponse.index());
        assertEquals(createIndexResponse.isShardsAcked(), parsedCreateIndexResponse.isShardsAcked());
        assertEquals(createIndexResponse.isAcknowledged(), parsedCreateIndexResponse.isAcknowledged());
    }

    /**
     * Returns a random {@link CreateIndexResponse}.
     */
    private static CreateIndexResponse createTestItem() throws IOException {
        boolean acknowledged = randomBoolean();
        boolean shardsAcked = acknowledged && randomBoolean();
        String index = randomAlphaOfLength(5);

        return new CreateIndexResponse(acknowledged, shardsAcked, index);
    }
}
