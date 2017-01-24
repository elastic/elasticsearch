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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetFieldMappingsResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings = new HashMap<>();
        FieldMappingMetaData fieldMappingMetaData = new FieldMappingMetaData("my field", new BytesArray("{}"), XContentType.JSON);
        mappings.put("index", Collections.singletonMap("type", Collections.singletonMap("field", fieldMappingMetaData)));
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            GetFieldMappingsResponse serialized = new GetFieldMappingsResponse();
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                serialized.readFrom(in);
                FieldMappingMetaData metaData = serialized.fieldMappings("index", "type", "field");
                assertNotNull(metaData);
                assertEquals(XContentType.JSON, metaData.getXContentType());
                assertEquals(new BytesArray("{}"), metaData.getSource());
            }
        }
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("AQVpbmRleAEEdHlwZQEFZmllbGQIbXkgZmllbGQCe30=");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
            Version.V_5_0_3_UNRELEASED, Version.V_5_1_1_UNRELEASED, Version.V_5_1_2_UNRELEASED, Version.V_5_2_0_UNRELEASED);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            GetFieldMappingsResponse response = new GetFieldMappingsResponse();
            response.readFrom(in);
            FieldMappingMetaData metaData = response.fieldMappings("index", "type", "field");
            assertNotNull(metaData);
            assertEquals(XContentType.JSON, metaData.getXContentType());
            assertEquals(new BytesArray("{}"), metaData.getSource());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                response.writeTo(out);
                assertArrayEquals(data, out.bytes().toBytesRef().bytes);
            }
        }
    }
}
