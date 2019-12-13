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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MappingsMergerTests extends ESTestCase {

    public void testMergeMappings_GivenIndicesWithIdenticalMappings() throws IOException {
        Map<String, Object> index1Properties = new HashMap<>();
        index1Properties.put("field_1", "field_1_mappings");
        index1Properties.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("properties", index1Properties);
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Properties = new HashMap<>();
        index2Properties.put("field_1", "field_1_mappings");
        index2Properties.put("field_2", "field_2_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("properties", index2Properties);
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetaData> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        assertThat(mergedMappings.valuesIt().next().getSourceAsMap(), equalTo(index1Mappings));
    }

    public void testMergeMappings_GivenIndicesWithDifferentTypes() throws IOException {
        Map<String, Object> index1Mappings = Collections.singletonMap("properties",
            Collections.singletonMap("field_1", "field_1_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("type_1", index1Mappings);

        Map<String, Object> index2Mappings = Collections.singletonMap("properties",
            Collections.singletonMap("field_1", "field_1_mappings"));
        MappingMetaData index2MappingMetaData = new MappingMetaData("type_2", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("type_1", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("type_2", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(newSource(), getMappingsResponse));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("source indices contain mappings for different types:"));
        assertThat(e.getMessage(), containsString("type_1"));
        assertThat(e.getMessage(), containsString("type_2"));
    }

    public void testMergeMappings_GivenFieldWithDifferentMapping() throws IOException {
        Map<String, Object> index1Mappings = Collections.singletonMap("properties",
            Collections.singletonMap("field_1", "field_1_mappings"));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Collections.singletonMap("properties",
            Collections.singletonMap("field_1", "different_field_1_mappings"));
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
            () -> MappingsMerger.mergeMappings(newSource(), getMappingsResponse));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("cannot merge mappings because of differences for field [field_1]"));
    }

    public void testMergeMappings_GivenIndicesWithDifferentMappingsButNoConflicts() throws IOException {
        Map<String, Object> index1Properties = new HashMap<>();
        index1Properties.put("field_1", "field_1_mappings");
        index1Properties.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("properties", index1Properties);
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", index1Mappings);

        Map<String, Object> index2Properties = new HashMap<>();
        index2Properties.put("field_1", "field_1_mappings");
        index2Properties.put("field_3", "field_3_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("properties", index2Properties);
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetaData);
        ImmutableOpenMap.Builder<String, MappingMetaData> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetaData> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

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

    public void testMergeMappings_GivenSourceFiltering() throws IOException {
        Map<String, Object> indexProperties = new HashMap<>();
        indexProperties.put("field_1", "field_1_mappings");
        indexProperties.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("properties", indexProperties);
        MappingMetaData indexMappingMetaData = new MappingMetaData("_doc", index1Mappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> indexMappingsMap = ImmutableOpenMap.builder();
        indexMappingsMap.put("_doc", indexMappingMetaData);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", indexMappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetaData> mergedMappings = MappingsMerger.mergeMappings(
            newSourceWithExcludes("field_1"), getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        Map<String, Object> mappingsAsMap = mergedMappings.valuesIt().next().getSourceAsMap();
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
