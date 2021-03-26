/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.indices.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetFieldMappingsResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            GetFieldMappingsResponseTests::createTestInstance,
            GetFieldMappingsResponseTests::toXContent,
            GetFieldMappingsResponse::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .test();
    }

    private Predicate<String> getRandomFieldsExcludeFilter() {
        // allow random fields at the level of `index` and `index.mappings.field`
        // otherwise random field could be evaluated as index name or type name
        return s -> false == (s.matches("(?<index>[^.]+)")
            || s.matches("(?<index>[^.]+)\\.mappings\\.(?<field>[^.]+)"));
    }

    private static GetFieldMappingsResponse createTestInstance() {
        Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        // if mappings is empty, means that fields are not found
        if (randomBoolean()) {
            int indices = randomInt(10);
            for (int i = 0; i < indices; i++) {
                Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
                int fields = randomInt(10);
                for (int k = 0; k < fields; k++) {
                    final String mapping = randomBoolean() ? "{\"type\":\"string\"}" : "{\"type\":\"keyword\"}";
                    final String fieldName = randomAlphaOfLength(8);
                    FieldMappingMetadata metadata = new FieldMappingMetadata(fieldName, new BytesArray(mapping));
                    fieldMappings.put(fieldName, metadata);
                }
                mappings.put(randomAlphaOfLength(8), fieldMappings);
            }
        }
        return new GetFieldMappingsResponse(mappings);
    }

    // As the client class GetFieldMappingsResponse doesn't have toXContent method, adding this method here only for the test
    private static void toXContent(GetFieldMappingsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Map<String, FieldMappingMetadata>> indexEntry : response.mappings().entrySet()) {
            builder.startObject(indexEntry.getKey());
            builder.startObject("mappings");
            for (Map.Entry<String, FieldMappingMetadata> fieldEntry : indexEntry.getValue().entrySet()) {
                builder.startObject(fieldEntry.getKey());
                builder.field("full_name", fieldEntry.getValue().fullName());
                builder.field("mapping", fieldEntry.getValue().sourceAsMap());
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
    }

}
