/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetMappingsResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse, GetMappingsResponse> {

    @Override
    protected org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse createServerTestInstance(XContentType xContentType) {
        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        int numberOfIndexes = randomIntBetween(1, 5);
        for (int i = 0; i < numberOfIndexes; i++) {
            mappings.put("index-" + randomAlphaOfLength(5), randomMappingMetadata());
        }
        return new org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse(mappings.build());
    }

    @Override
    protected GetMappingsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetMappingsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse serverTestInstance,
                                   GetMappingsResponse clientInstance) {
        assertMapEquals(serverTestInstance.getMappings(), clientInstance.mappings());
    }

    public static MappingMetadata randomMappingMetadata() {
        Map<String, Object> mappings = new HashMap<>();

        if (frequently()) { // rarely have no fields
            mappings.put("field1", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
        }

        return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings);
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

}
