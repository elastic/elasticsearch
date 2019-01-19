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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetMappingsResponseTests extends AbstractStreamableXContentTestCase<GetMappingsResponse> {

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
    protected GetMappingsResponse createBlankInstance() {
        return new GetMappingsResponse();
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

    @Override
    protected GetMappingsResponse createTestInstance() {
        return createTestInstance(true);
    }

    private GetMappingsResponse createTestInstance(boolean randomTypeNames) {
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexBuilder = ImmutableOpenMap.builder();
        int typeCount = rarely() ? 0 : 1;
        indexBuilder.put("index-" + randomAlphaOfLength(5), createMappingsForIndex(typeCount, randomTypeNames));
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

    @Override
    protected GetMappingsResponse createXContextTestInstance(XContentType xContentType) {
        // don't use random type names for XContent roundtrip tests because we cannot parse them back anymore
        return createTestInstance(false);
    }

    /**
     * check that the "old" legacy response format with types works as expected
     */
    public void testToXContentWithTypes() throws IOException {
        Params params = new ToXContent.MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "true"));
        xContentTester(this::createParser, t -> createTestInstance(), params, this::fromXContentWithTypes)
                .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
                .supportsUnknownFields(supportsUnknownFields())
                .shuffleFieldsExceptions(getShuffleFieldsExceptions())
                .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
                .assertEqualsConsumer(this::assertEqualInstances)
                .assertToXContentEquivalence(true)
                .test();
    }

    /**
     * including the pre-7.0 parsing code here to test that older HLRC clients using this can parse the responses that are
     * returned when "include_type_name=true"
     */
    private GetMappingsResponse fromXContentWithTypes(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        Map<String, Object> parts = parser.map();

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = new ImmutableOpenMap.Builder<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            final String indexName = entry.getKey();
            assert entry.getValue() instanceof Map : "expected a map as type mapping, but got: " + entry.getValue().getClass();
            final Map<String, Object> mapping = (Map<String, Object>) ((Map) entry.getValue()).get("mappings");

            ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = new ImmutableOpenMap.Builder<>();
            for (Map.Entry<String, Object> typeEntry : mapping.entrySet()) {
                final String typeName = typeEntry.getKey();
                assert typeEntry.getValue() instanceof Map : "expected a map as inner type mapping, but got: "
                        + typeEntry.getValue().getClass();
                final Map<String, Object> fieldMappings = (Map<String, Object>) typeEntry.getValue();
                MappingMetaData mmd = new MappingMetaData(typeName, fieldMappings);
                typeBuilder.put(typeName, mmd);
            }
            builder.put(indexName, typeBuilder.build());
        }

        return new GetMappingsResponse(builder.build());
    }
}
