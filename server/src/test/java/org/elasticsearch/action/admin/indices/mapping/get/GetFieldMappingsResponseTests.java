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

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetFieldMappingsResponseTests extends AbstractWireSerializingTestCase<GetFieldMappingsResponse> {

    public void testManualSerialization() throws IOException {
        Map<String, Map<String, FieldMappingMetaData>> mappings = new HashMap<>();
        FieldMappingMetaData fieldMappingMetaData = new FieldMappingMetaData("my field", new BytesArray("{}"));
        mappings.put("index", Collections.singletonMap("field", fieldMappingMetaData));
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                GetFieldMappingsResponse serialized = new GetFieldMappingsResponse(in);
                FieldMappingMetaData metaData = serialized.fieldMappings("index", "field");
                assertNotNull(metaData);
                assertEquals(new BytesArray("{}"), metaData.getSource());
            }
        }
    }

    public void testNullFieldMappingToXContent() {
        Map<String, Map<String, FieldMappingMetaData>> mappings = new HashMap<>();
        mappings.put("index", Collections.emptyMap());
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);
        assertEquals("{\"index\":{\"mappings\":{}}}", Strings.toString(response));
    }

    @Override
    protected GetFieldMappingsResponse createTestInstance() {
        return new GetFieldMappingsResponse(randomMapping());
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> instanceReader() {
        return GetFieldMappingsResponse::new;
    }

    private Map<String, Map<String, FieldMappingMetaData>> randomMapping() {
        Map<String, Map<String, FieldMappingMetaData>> mappings = new HashMap<>();

        int indices = randomInt(10);
        for(int i = 0; i < indices; i++) {
            Map<String, FieldMappingMetaData> fieldMappings = new HashMap<>();
            int fields = randomInt(10);
            for (int k = 0; k < fields; k++) {
                final String mapping = randomBoolean() ? "{\"type\":\"string\"}" : "{\"type\":\"keyword\"}";
                FieldMappingMetaData metaData =
                    new FieldMappingMetaData("my field", new BytesArray(mapping));
                fieldMappings.put("field" + k, metaData);
            }
            mappings.put("index" + i, fieldMappings);
        }
        return mappings;
    }
}
