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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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

    public void testFromXContent() throws IOException {
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
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());

        final Tuple<XContentBuilder, CreateIndexResponse> tuple = randomCreateIndexResponse(xContentType, humanReadable);
        XContentBuilder CreateIndexResponseXContent = tuple.v1();
        CreateIndexResponse expectedCreateIndexResponse = tuple.v2();

        BytesReference originalBytes = CreateIndexResponseXContent.bytes();

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

        asserCreateIndexResponse(expectedCreateIndexResponse, parsedCreateIndexResponse);
    }

    public static void asserCreateIndexResponse(CreateIndexResponse expected, CreateIndexResponse actual) {
        assertEquals(expected.index(), actual.index());
        assertEquals(expected.isShardsAcked(), actual.isShardsAcked());
        assertEquals(expected.isAcknowledged(), actual.isAcknowledged());
    }

    /**
     * Returns a tuple of an {@link XContentBuilder} and a {@link CreateIndexResponse}.
     * <p>
     * The left element is the actual {@link XContentBuilder} to serialize while the right element is the
     * expected {@link CreateIndexResponse} after parsing.
     */
    public static Tuple<XContentBuilder, CreateIndexResponse> randomCreateIndexResponse(
        XContentType xContentType, boolean humanReadable) throws IOException {

        boolean acknowledged = randomBoolean();
        boolean shardsAcked = acknowledged && randomBoolean();
        String index = randomAlphaOfLength(5);

        XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.humanReadable(humanReadable);
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.field("shards_acknowledged", shardsAcked);
        builder.field("index", index);
        builder.endObject();

        CreateIndexResponse expected = new CreateIndexResponse(acknowledged, shardsAcked, index);

        return Tuple.tuple(builder, expected);
    }
}
