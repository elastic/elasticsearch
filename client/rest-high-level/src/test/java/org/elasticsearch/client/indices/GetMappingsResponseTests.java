/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.client.indices.GetMappingsResponse.MAPPINGS;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetMappingsResponseTests extends ESTestCase {

    // Because the client-side class does not have a toXContent method, we test xContent serialization by creating
    // a random client object, converting it to a server object then serializing it to xContent, and finally
    // parsing it back as a client object. We check equality between the original client object, and the parsed one.
    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            GetMappingsResponseTests::createTestInstance,
            GetMappingsResponseTests::toXContent,
            GetMappingsResponse::fromXContent
        ).supportsUnknownFields(true)
            .assertEqualsConsumer(GetMappingsResponseTests::assertEqualInstances)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter())
            .test();
    }

    private static GetMappingsResponse createTestInstance() {
        Map<String, MappingMetadata> mappings = Collections.singletonMap("index-" + randomAlphaOfLength(5), randomMappingMetadata());
        return new GetMappingsResponse(mappings);
    }

    private static void assertEqualInstances(GetMappingsResponse expected, GetMappingsResponse actual) {
        assertEquals(expected.mappings(), actual.mappings());
    }

    private Predicate<String> randomFieldsExcludeFilter() {
        return field -> field.equals(MAPPINGS.getPreferredName()) == false;
    }

    public static MappingMetadata randomMappingMetadata() {
        Map<String, Object> mappings = new HashMap<>();

        if (frequently()) { // rarely have no fields
            mappings.put("field1", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
        }

        try {
            return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomFrom("text", "keyword"));
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        }
        return mappings;
    }

    private static void toXContent(GetMappingsResponse response, XContentBuilder builder) throws IOException {
        Params params = new ToXContent.MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "false"));
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> allMappings = ImmutableOpenMap.builder();

        for (Map.Entry<String, MappingMetadata> indexEntry : response.mappings().entrySet()) {
            ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
            mappings.put(MapperService.SINGLE_MAPPING_NAME, indexEntry.getValue());
            allMappings.put(indexEntry.getKey(), mappings.build());
        }

        org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse serverResponse =
            new org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse(allMappings.build());

        builder.startObject();
        serverResponse.toXContent(builder, params);
        builder.endObject();
    }
}
