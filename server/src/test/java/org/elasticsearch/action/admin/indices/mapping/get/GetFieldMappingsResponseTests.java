/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;

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
                assertEquals(new BytesArray("{}"), metadata.getSource());
            }
        }
    }

    public void testNullFieldMappingToXContent() {
        Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        mappings.put("index", Collections.emptyMap());
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);
        assertEquals("{\"index\":{\"mappings\":{}}}", Strings.toString(response));
    }

    public void testToXContentIncludesType() throws Exception {
        Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        FieldMappingMetadata fieldMappingMetadata = new FieldMappingMetadata("my field", new BytesArray("{}"));
        mappings.put("index", Collections.singletonMap("field", fieldMappingMetadata));
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER,
            "true"));

        // v7 with  include_type_name attaches _doc
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent, RestApiVersion.V_7)) {
            response.toXContent(builder, params);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Map<String, Object>>> index =
                    (Map<String, Map<String, Map<String, Object>>>) parser.map().get("index");
                assertThat(index.get("mappings"), hasKey(MapperService.SINGLE_MAPPING_NAME));
                assertThat(index.get("mappings").get(MapperService.SINGLE_MAPPING_NAME), hasKey("field"));
            }
        }

        // v7 with no include_type_name do not attach _doc
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent, RestApiVersion.V_7)) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> index = (Map<String, Map<String, Object>>) parser.map().get("index");
                assertThat(index.get("mappings"), hasKey("field"));
            }
        }
        //v8 does not have _doc, even when include_type_name is present
        // (although this throws unconsumed parameter exception in RestGetFieldMappingsAction)
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent, RestApiVersion.V_8)) {
            response.toXContent(builder, params);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> index = (Map<String, Map<String, Object>>) parser.map().get("index");
                assertThat(index.get("mappings"), hasKey("field"));
            }
        }

        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent, RestApiVersion.V_8)) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> index = (Map<String, Map<String, Object>>) parser.map().get("index");
                assertThat(index.get("mappings"), hasKey("field"));
            }
        }
    }

    @Override
    protected GetFieldMappingsResponse createTestInstance() {
        return new GetFieldMappingsResponse(randomMapping());
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
                FieldMappingMetadata metadata =
                    new FieldMappingMetadata("my field", new BytesArray(mapping));
                fieldMappings.put("field" + k, metadata);
            }
            mappings.put("index" + i, fieldMappings);
        }
        return mappings;
    }
}
