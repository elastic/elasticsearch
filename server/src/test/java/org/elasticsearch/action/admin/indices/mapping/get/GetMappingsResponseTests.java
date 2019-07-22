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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetMappingsResponseTests extends AbstractSerializingTestCase<GetMappingsResponse> {

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testCheckEqualsAndHashCode() {
        GetMappingsResponse resp = createTestInstance();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(resp, r -> new GetMappingsResponse(r.mappings()), GetMappingsResponseTests::mutate);
    }

    @Override
    protected GetMappingsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetMappingsResponse.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<GetMappingsResponse> instanceReader() {
        return GetMappingsResponse::new;
    }

    private static GetMappingsResponse mutate(GetMappingsResponse original) throws IOException {
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = ImmutableOpenMap.builder(original.mappings());
        String indexKey = original.mappings().keys().iterator().next().value;

        ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = ImmutableOpenMap.builder(original.mappings().get(indexKey));
        final String typeKey;
        Iterator<ObjectCursor<String>> iter = original.mappings().get(indexKey).keys().iterator();
        if (iter.hasNext()) {
            typeKey = iter.next().value;
        } else {
            typeKey = "new-type";
        }

        typeBuilder.put(typeKey, new MappingMetaData("type-" + randomAlphaOfLength(6), randomFieldMapping()));

        builder.put(indexKey, typeBuilder.build());
        return new GetMappingsResponse(builder.build());
    }

    @Override
    protected GetMappingsResponse mutateInstance(GetMappingsResponse instance) throws IOException {
        return mutate(instance);
    }

    public static ImmutableOpenMap<String, MappingMetaData> createMappingsForIndex(int typeCount, boolean randomTypeName) {
        List<MappingMetaData> typeMappings = new ArrayList<>(typeCount);

        for (int i = 0; i < typeCount; i++) {
            if (rarely() == false) { // rarely have no fields
                Map<String, Object> mappings = new HashMap<>();
                mappings.put("field-" + i, randomFieldMapping());
                if (randomBoolean()) {
                    mappings.put("field2-" + i, randomFieldMapping());
                }

                try {
                    String typeName = MapperService.SINGLE_MAPPING_NAME;
                    if (randomTypeName) {
                        typeName = "type-" + randomAlphaOfLength(5);
                    }
                    MappingMetaData mmd = new MappingMetaData(typeName, mappings);
                    typeMappings.add(mmd);
                } catch (IOException e) {
                    fail("shouldn't have failed " + e);
                }
            }
        }
        ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = ImmutableOpenMap.builder();
        typeMappings.forEach(mmd -> typeBuilder.put(mmd.type(), mmd));
        return typeBuilder.build();
    }

    /**
     * For xContent roundtrip testing we force the xContent output to still contain types because the parser
     * still expects them. The new typeless parsing is implemented in the client side GetMappingsResponse.
     */
    @Override
    protected Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "true"));
    }

    @Override
    protected GetMappingsResponse createTestInstance() {
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexBuilder = ImmutableOpenMap.builder();
        int typeCount = rarely() ? 0 : 1;
        indexBuilder.put("index-" + randomAlphaOfLength(5), createMappingsForIndex(typeCount, randomBoolean()));
        GetMappingsResponse resp = new GetMappingsResponse(indexBuilder.build());
        logger.debug("--> created: {}", resp);
        return resp;
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
