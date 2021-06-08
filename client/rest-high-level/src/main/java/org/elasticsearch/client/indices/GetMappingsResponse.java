/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetMappingsResponse {

    static final ParseField MAPPINGS = new ParseField("mappings");

    private Map<String, MappingMetadata> mappings;

    public GetMappingsResponse(Map<String, MappingMetadata> mappings) {
        this.mappings = mappings;
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public static GetMappingsResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        XContentParserUtils.ensureExpectedToken(parser.currentToken(),
            XContentParser.Token.START_OBJECT,
            parser);

        Map<String, Object> parts = parser.map();

        Map<String, MappingMetadata> mappings = new HashMap<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            String indexName = entry.getKey();
            assert entry.getValue() instanceof Map : "expected a map as type mapping, but got: " + entry.getValue().getClass();

            @SuppressWarnings("unchecked")
            final Map<String, Object> fieldMappings = (Map<String, Object>) ((Map<String, ?>) entry.getValue())
                    .get(MAPPINGS.getPreferredName());

            mappings.put(indexName, new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, fieldMappings));
        }

        return new GetMappingsResponse(mappings);
    }
}
