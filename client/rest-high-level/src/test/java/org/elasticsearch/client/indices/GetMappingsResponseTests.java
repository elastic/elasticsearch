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

package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.ESTestCase;

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
            GetMappingsResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertEqualsConsumer(GetMappingsResponseTests::assertEqualInstances)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter())
            .test();
    }

    private static GetMappingsResponse createTestInstance() {
        Map<String, MappingMetaData> mappings = Collections.singletonMap(
            "index-" + randomAlphaOfLength(5), randomMappingMetaData());
        return new GetMappingsResponse(mappings);
    }

    private static void assertEqualInstances(GetMappingsResponse expected, GetMappingsResponse actual) {
        assertEquals(expected.mappings(), actual.mappings());
    }

    private Predicate<String> randomFieldsExcludeFilter() {
        return field -> !field.equals(MAPPINGS.getPreferredName());
    }

    public static MappingMetaData randomMappingMetaData() {
        Map<String, Object> mappings = new HashMap<>();

        if (frequently()) { // rarely have no fields
            mappings.put("field1", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
        }

        try {
            return new MappingMetaData(MapperService.SINGLE_MAPPING_NAME, mappings);
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
        Params params = new ToXContent.MapParams(
            Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "false"));
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = ImmutableOpenMap.builder();

        for (Map.Entry<String, MappingMetaData> indexEntry : response.mappings().entrySet()) {
            ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
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
