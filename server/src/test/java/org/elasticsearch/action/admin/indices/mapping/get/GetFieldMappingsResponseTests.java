/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
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
        Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        FieldMappingMetadata fieldMappingMetadata = new FieldMappingMetadata("my field", new BytesArray("{}"));
        mappings.put("index", Collections.singletonMap("field", fieldMappingMetadata));
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                GetFieldMappingsResponse serialized = new GetFieldMappingsResponse(in);
                FieldMappingMetadata metadata = serialized.fieldMappings("index", "field");
                assertNotNull(metadata);
                assertEquals(new BytesArray("{}"), metadata.source());
            }
        }
    }

    public void testNullFieldMappingToXContent() {
        Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        mappings.put("index", Collections.emptyMap());
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);
        assertEquals("{\"index\":{\"mappings\":{}}}", Strings.toString(response));
    }

    @Override
    protected GetFieldMappingsResponse createTestInstance() {
        return new GetFieldMappingsResponse(randomMapping());
    }

    @Override
    protected GetFieldMappingsResponse mutateInstance(GetFieldMappingsResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> instanceReader() {
        return GetFieldMappingsResponse::new;
    }

    private Map<String, Map<String, FieldMappingMetadata>> randomMapping() {
        Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();

        int indices = randomInt(10);
        for (int i = 0; i < indices; i++) {
            Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
            int fields = randomInt(10);
            for (int k = 0; k < fields; k++) {
                final String mapping = randomBoolean() ? "{\"type\":\"string\"}" : "{\"type\":\"keyword\"}";
                FieldMappingMetadata metadata = new FieldMappingMetadata("my field", new BytesArray(mapping));
                fieldMappings.put("field" + k, metadata);
            }
            mappings.put("index" + i, fieldMappings);
        }
        return mappings;
    }
}
