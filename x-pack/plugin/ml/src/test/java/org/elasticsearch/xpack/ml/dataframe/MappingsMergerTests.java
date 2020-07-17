/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MappingsMergerTests extends ESTestCase {

    public void testMergeMappings_GivenIndicesWithIdenticalMappings() {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetadata);
        mappings.put("index_2", index2MappingMetadata);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        MappingMetadata mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        assertThat(mergedMappings.getSourceAsMap(), equalTo(index1Mappings));
    }

    public void testMergeMappings_GivenFieldWithDifferentMapping() {
        Map<String, Object> index1Mappings = Map.of("properties", Map.of("field_1", "field_1_mappings"));
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties", Map.of("field_1", "different_field_1_mappings"));
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetadata);
        mappings.put("index_2", index2MappingMetadata);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(newSource(), getMappingsResponse));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("cannot merge mappings because of differences for field [field_1]; "));
        assertThat(e.getMessage(), containsString("mapped as [different_field_1_mappings] in index [index_2]"));
        assertThat(e.getMessage(), containsString("mapped as [field_1_mappings] in index [index_1]"));
    }

    public void testMergeMappings_GivenIndicesWithDifferentMappingsButNoConflicts() {
        Map<String, Object> index1Mappings = Map.of("properties",
            Map.of("field_1", "field_1_mappings", "field_2", "field_2_mappings"));
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Map.of("properties",
            Map.of("field_1", "field_1_mappings", "field_3", "field_3_mappings"));
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetadata);
        mappings.put("index_2", index2MappingMetadata);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        MappingMetadata mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

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
        MappingMetadata indexMappingMetadata = new MappingMetadata("_doc", indexMappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        mappings.put("index", indexMappingMetadata);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        MappingMetadata mergedMappings = MappingsMerger.mergeMappings(
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
