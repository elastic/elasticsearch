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
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MappingsMergerTests extends ESTestCase {

    public void testMergeMappings_GivenIndicesWithIdenticalMappings() {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetaData);
        mappings.put("index_2", index2MappingMetaData);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        MappingMetaData mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        assertThat(mergedMappings.getSourceAsMap(), equalTo(index1Mappings));
    }

    public void testMergeMappings_GivenFieldWithDifferentMapping() {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "different_field_1_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetaData);
        mappings.put("index_2", index2MappingMetaData);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(newSource(), getMappingsResponse));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("cannot merge mappings because of differences for field [field_1]"));
    }

    public void testMergeMappings_GivenIndicesWithDifferentMappingsButNoConflicts() {
        Map<String, Object> index1Mappings = Map.of("properties",
            Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties",
            Map.of("field_1", "field_1_mappings", "field_3", "field_3_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetaData);
        mappings.put("index_2", index2MappingMetaData);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        MappingMetaData mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        Map<String, Object> mappingsAsMap = mergedMappings.getSourceAsMap();
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

    public void testMergeMappings_GivenSourceFiltering() {
        Map<String, Object> indexMappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetaData indexMappingMetaData = new MappingMetaData("_doc", indexMappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("index", indexMappingMetaData);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        MappingMetaData mergedMappings = MappingsMerger.mergeMappings(
            newSourceWithExcludes("field_1"), getMappingsResponse);

        Map<String, Object> mappingsAsMap = mergedMappings.getSourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldMappings = (Map<String, Object>) mappingsAsMap.get("properties");

        assertThat(fieldMappings.size(), equalTo(1));
        assertThat(fieldMappings.containsKey("field_2"), is(true));
    }

    private static DataFrameAnalyticsSource newSource() {
        return new DataFrameAnalyticsSource(new String[] {"index"}, null, null);
    }

    private static DataFrameAnalyticsSource newSourceWithExcludes(String... excludes) {
        return new DataFrameAnalyticsSource(new String[] {"index"}, null,
            new FetchSourceContext(true, null, excludes));
    }
}
