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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Base64;

public class CreateIndexRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("foo");
        BytesReference bytesReference = JsonXContent.contentBuilder().startObject().startObject("type").endObject().endObject().bytes();
        request.mapping("my_type", bytesReference, XContentType.JSON);

        assertEquals(XContentType.JSON, request.mappings().get("my_type").v1());
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                CreateIndexRequest serialized = new CreateIndexRequest();
                serialized.readFrom(in);
                assertEquals(XContentType.JSON, serialized.mappings().get("my_type").v1());
                assertEquals(request.index(), serialized.index());
                assertEquals(bytesReference, serialized.mappings().get("my_type").v2());
            }
        }
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("ADwDAANmb28APAMBB215X3R5cGULeyJ0eXBlIjp7fX0AAAD////+AA==");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
            Version.V_5_0_3_UNRELEASED, Version.V_5_1_1_UNRELEASED, Version.V_5_1_2_UNRELEASED, Version.V_5_2_0_UNRELEASED);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            CreateIndexRequest serialized = new CreateIndexRequest();
            serialized.readFrom(in);
            assertEquals(XContentType.JSON, serialized.mappings().get("my_type").v1());
            assertEquals("foo", serialized.index());
            BytesReference bytesReference = JsonXContent.contentBuilder().startObject().startObject("type").endObject().endObject().bytes();
            assertEquals(bytesReference, serialized.mappings().get("my_type").v2());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                serialized.writeTo(out);
                out.flush();
                assertArrayEquals(data, out.bytes().toBytesRef().bytes);
            }
        }
    }
}
