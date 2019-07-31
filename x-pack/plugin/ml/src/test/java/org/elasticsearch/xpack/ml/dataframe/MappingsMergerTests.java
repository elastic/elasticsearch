/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MappingsMergerTests extends ESTestCase {

    public void testMergeMappings_GivenIndicesWithIdenticalMappings() throws IOException {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetaData> mergedMappings = MappingsMerger.mergeMappings(getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        assertThat(mergedMappings.valuesIt().next().getSourceAsMap(), equalTo(index1Mappings));
    }

    public void testMergeMappings_GivenIndicesWithDifferentTypes() throws IOException {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("type_1", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("type_2", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(getMappingsResponse));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("source indices contain mappings for different types:"));
        assertThat(e.getMessage(), containsString("type_1"));
        assertThat(e.getMessage(), containsString("type_2"));
    }

    public void testMergeMappings_GivenFieldWithDifferentMapping() throws IOException {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "different_field_1_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(getMappingsResponse));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("cannot merge mappings because of differences for field [field_1]"));
    }

    public void testMergeMappings_GivenIndicesWithDifferentMappingsButNoConflicts() throws IOException {
        Map<String, Object> index1Mappings = Map.of("properties",
            Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties",
            Map.of("field_1", "field_1_mappings", "field_3", "field_3_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetaData> mergedMappings = MappingsMerger.mergeMappings(getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        Map<String, Object> mappingsAsMap = mergedMappings.valuesIt().next().getSourceAsMap();
        assertThat(mappingsAsMap.size(), equalTo(1));
        assertThat(mappingsAsMap.containsKey("properties"), is(true));

        @SuppressWarnings("unchecked")
        Map<String, Object> fieldMappings = (Map<String, Object>) mappingsAsMap.get("properties");

        assertThat(fieldMappings.size(), equalTo(3));
        assertThat(fieldMappings.keySet(), containsInAnyOrder("field_1", "field_2", "field_3"));
        assertThat(fieldMappings.get("field_1"), equalTo("field_1_mappings"));
        assertThat(fieldMappings.get("field_2"), equalTo("field_2_mappings"));
        assertThat(fieldMappings.get("field_3"), equalTo("field_3_mappings"));
    }
}
