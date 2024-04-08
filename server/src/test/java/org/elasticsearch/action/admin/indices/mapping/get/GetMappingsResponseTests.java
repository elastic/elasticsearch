/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GetMappingsResponseTests extends AbstractWireSerializingTestCase<GetMappingsResponse> {

    public void testCheckEqualsAndHashCode() {
        GetMappingsResponse resp = createTestInstance();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(resp, r -> new GetMappingsResponse(r.mappings()), GetMappingsResponseTests::mutate);
    }

    @Override
    protected Writeable.Reader<GetMappingsResponse> instanceReader() {
        return GetMappingsResponse::new;
    }

    private static GetMappingsResponse mutate(GetMappingsResponse original) {
        Map<String, MappingMetadata> builder = new HashMap<>(original.mappings());
        String indexKey = original.mappings().keySet().iterator().next();
        builder.put(indexKey + "1", createMappingsForIndex());
        return new GetMappingsResponse(builder);
    }

    @Override
    protected GetMappingsResponse mutateInstance(GetMappingsResponse instance) {
        return mutate(instance);
    }

    public static MappingMetadata createMappingsForIndex() {
        Map<String, Object> mappings = new HashMap<>();
        if (rarely() == false) { // rarely have no fields
            mappings.put("field", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
            String typeName = MapperService.SINGLE_MAPPING_NAME;
            return new MappingMetadata(typeName, mappings);
        }
        return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings);
    }

    @Override
    protected GetMappingsResponse createTestInstance() {
        GetMappingsResponse resp = new GetMappingsResponse(Map.of("index-" + randomAlphaOfLength(5), createMappingsForIndex()));
        logger.debug("--> created: {}", resp);
        return resp;
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new GetMappingsResponse(
                IntStream.range(0, randomIntBetween(1, 10))
                    .mapToObj(i -> "index-" + i)
                    .collect(Collectors.toUnmodifiableMap(Function.identity(), k -> createMappingsForIndex()))
            ),
            response -> response.mappings().size() + 2
        );
    }

    // Not meant to be exhaustive
    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomBoolean() ? "text" : "keyword");
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else if (randomBoolean()) {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        } else if (randomBoolean()) {
            mappings.put("type", "object");
            mappings.put("dynamic", "strict");
            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> props1 = new HashMap<>();
            props1.put("type", randomFrom("text", "keyword"));
            props1.put("analyzer", "keyword");
            properties.put("subtext", props1);
            Map<String, Object> props2 = new HashMap<>();
            props2.put("type", "object");
            Map<String, Object> prop2properties = new HashMap<>();
            Map<String, Object> props3 = new HashMap<>();
            props3.put("type", "integer");
            props3.put("index", "false");
            prop2properties.put("subsubfield", props3);
            props2.put("properties", prop2properties);
            mappings.put("properties", properties);
        } else {
            mappings.put("type", "keyword");
        }
        return mappings;
    }
}
