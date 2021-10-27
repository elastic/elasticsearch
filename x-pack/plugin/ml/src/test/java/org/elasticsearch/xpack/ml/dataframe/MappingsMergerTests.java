/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MappingsMergerTests extends ESTestCase {

    public void testMergeMappings_GivenIndicesWithIdenticalProperties() throws IOException {
        Map<String, Object> index1Properties = new HashMap<>();
        index1Properties.put("field_1", "field_1_mappings");
        index1Properties.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("properties", index1Properties);
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Properties = new HashMap<>();
        index2Properties.put("field_1", "field_1_mappings");
        index2Properties.put("field_2", "field_2_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("properties", index2Properties);
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetadata> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        Map<String, Object> expectedMappings = new HashMap<>();
        expectedMappings.put("dynamic", false);
        expectedMappings.put("properties", index1Mappings.get("properties"));

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        assertThat(mergedMappings.valuesIt().next().getSourceAsMap(), equalTo(expectedMappings));
    }

    public void testMergeMappings_GivenPropertyFieldWithDifferentMapping() throws IOException {
        Map<String, Object> index1Mappings = Collections.singletonMap(
            "properties",
            Collections.singletonMap("field_1", "field_1_mappings")
        );
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Mappings = Collections.singletonMap(
            "properties",
            Collections.singletonMap("field_1", "different_field_1_mappings")
        );
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(newSource(), getMappingsResponse)
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("cannot merge [properties] mappings because of differences for field [field_1]; "));
        assertThat(e.getMessage(), containsString("mapped as [different_field_1_mappings] in index [index_2]"));
        assertThat(e.getMessage(), containsString("mapped as [field_1_mappings] in index [index_1]"));
    }

    public void testMergeMappings_GivenIndicesWithDifferentPropertiesButNoConflicts() throws IOException {
        Map<String, Object> index1Properties = new HashMap<>();
        index1Properties.put("field_1", "field_1_mappings");
        index1Properties.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("properties", index1Properties);
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Properties = new HashMap<>();
        index2Properties.put("field_1", "field_1_mappings");
        index2Properties.put("field_3", "field_3_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("properties", index2Properties);
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetadata> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        Map<String, Object> mappingsAsMap = mergedMappings.valuesIt().next().getSourceAsMap();
        assertThat(mappingsAsMap.keySet(), containsInAnyOrder("dynamic", "properties"));
        assertThat(mappingsAsMap.get("dynamic"), equalTo(false));

        @SuppressWarnings("unchecked")
        Map<String, Object> fieldMappings = (Map<String, Object>) mappingsAsMap.get("properties");

        assertThat(fieldMappings.keySet(), containsInAnyOrder("field_1", "field_2", "field_3"));
        assertThat(fieldMappings.get("field_1"), equalTo("field_1_mappings"));
        assertThat(fieldMappings.get("field_2"), equalTo("field_2_mappings"));
        assertThat(fieldMappings.get("field_3"), equalTo("field_3_mappings"));
    }

    public void testMergeMappings_GivenIndicesWithIdenticalRuntimeFields() throws IOException {
        Map<String, Object> index1Runtime = new HashMap<>();
        index1Runtime.put("field_1", "field_1_mappings");
        index1Runtime.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("runtime", index1Runtime);
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Runtime = new HashMap<>();
        index2Runtime.put("field_1", "field_1_mappings");
        index2Runtime.put("field_2", "field_2_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("runtime", index2Runtime);
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetadata> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        Map<String, Object> expectedMappings = new HashMap<>();
        expectedMappings.put("dynamic", false);
        expectedMappings.put("runtime", index1Mappings.get("runtime"));

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        assertThat(mergedMappings.valuesIt().next().getSourceAsMap(), equalTo(expectedMappings));
    }

    public void testMergeMappings_GivenRuntimeFieldWithDifferentMapping() throws IOException {
        Map<String, Object> index1Runtime = new HashMap<>();
        index1Runtime.put("field_1", "field_1_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("runtime", index1Runtime);
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Runtime = new HashMap<>();
        index2Runtime.put("field_1", "different_field_1_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("runtime", index2Runtime);
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> MappingsMerger.mergeMappings(newSource(), getMappingsResponse)
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("cannot merge [runtime] mappings because of differences for field [field_1]; "));
        assertThat(e.getMessage(), containsString("mapped as [different_field_1_mappings] in index [index_2]"));
        assertThat(e.getMessage(), containsString("mapped as [field_1_mappings] in index [index_1]"));
    }

    public void testMergeMappings_GivenIndicesWithDifferentRuntimeFieldsButNoConflicts() throws IOException {
        Map<String, Object> index1Runtime = new HashMap<>();
        index1Runtime.put("field_1", "field_1_mappings");
        index1Runtime.put("field_2", "field_2_mappings");
        Map<String, Object> index1Mappings = Collections.singletonMap("runtime", index1Runtime);
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Runtime = new HashMap<>();
        index2Runtime.put("field_1", "field_1_mappings");
        index2Runtime.put("field_3", "field_3_mappings");
        Map<String, Object> index2Mappings = Collections.singletonMap("runtime", index2Runtime);
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetadata> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        Map<String, Object> mappingsAsMap = mergedMappings.valuesIt().next().getSourceAsMap();
        assertThat(mappingsAsMap.keySet(), containsInAnyOrder("dynamic", "runtime"));
        assertThat(mappingsAsMap.get("dynamic"), equalTo(false));

        @SuppressWarnings("unchecked")
        Map<String, Object> runtimeFields = (Map<String, Object>) mappingsAsMap.get("runtime");

        assertThat(runtimeFields.keySet(), containsInAnyOrder("field_1", "field_2", "field_3"));
        assertThat(runtimeFields.get("field_1"), equalTo("field_1_mappings"));
        assertThat(runtimeFields.get("field_2"), equalTo("field_2_mappings"));
        assertThat(runtimeFields.get("field_3"), equalTo("field_3_mappings"));
    }

    public void testMergeMappings_GivenPropertyAndRuntimeFields() throws IOException {
        Map<String, Object> index1Mappings = new HashMap<>();
        {
            Map<String, Object> index1Properties = new HashMap<>();
            index1Properties.put("p_1", "p_1_mappings");
            Map<String, Object> index1Runtime = new HashMap<>();
            index1Runtime.put("r_1", "r_1_mappings");
            index1Mappings.put("properties", index1Properties);
            index1Mappings.put("runtime", index1Runtime);
        }
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", index1Mappings);

        Map<String, Object> index2Mappings = new HashMap<>();
        {
            Map<String, Object> index2Properties = new HashMap<>();
            index2Properties.put("p_2", "p_2_mappings");
            Map<String, Object> index2Runtime = new HashMap<>();
            index2Runtime.put("r_2", "r_2_mappings");
            index2Runtime.put("p_1", "p_1_different_mappings"); // It is ok to have conflicting runtime/property mappings
            index2Mappings.put("properties", index2Properties);
            index2Mappings.put("runtime", index2Runtime);
        }
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", index2Mappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> index1MappingsMap = ImmutableOpenMap.builder();
        index1MappingsMap.put("_doc", index1MappingMetadata);
        ImmutableOpenMap.Builder<String, MappingMetadata> index2MappingsMap = ImmutableOpenMap.builder();
        index2MappingsMap.put("_doc", index2MappingMetadata);

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingsMap.build());
        mappings.put("index_2", index2MappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetadata> mergedMappings = MappingsMerger.mergeMappings(newSource(), getMappingsResponse);

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        Map<String, Object> mappingsAsMap = mergedMappings.valuesIt().next().getSourceAsMap();
        assertThat(mappingsAsMap.keySet(), containsInAnyOrder("dynamic", "properties", "runtime"));
        assertThat(mappingsAsMap.get("dynamic"), equalTo(false));

        @SuppressWarnings("unchecked")
        Map<String, Object> mergedProperties = (Map<String, Object>) mappingsAsMap.get("properties");
        assertThat(mergedProperties.keySet(), containsInAnyOrder("p_1", "p_2"));
        assertThat(mergedProperties.get("p_1"), equalTo("p_1_mappings"));
        assertThat(mergedProperties.get("p_2"), equalTo("p_2_mappings"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mergedRuntime = (Map<String, Object>) mappingsAsMap.get("runtime");
        assertThat(mergedRuntime.keySet(), containsInAnyOrder("r_1", "r_2", "p_1"));
        assertThat(mergedRuntime.get("r_1"), equalTo("r_1_mappings"));
        assertThat(mergedRuntime.get("r_2"), equalTo("r_2_mappings"));
        assertThat(mergedRuntime.get("p_1"), equalTo("p_1_different_mappings"));
    }

    public void testMergeMappings_GivenSourceFiltering() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("field_1", "field_1_mappings");
        properties.put("field_2", "field_2_mappings");

        Map<String, Object> runtime = new HashMap<>();
        runtime.put("runtime_field_1", "runtime_field_1_mappings");
        runtime.put("runtime_field_2", "runtime_field_2_mappings");

        Map<String, Object> indexMappings = new HashMap<>();
        indexMappings.put("properties", properties);
        indexMappings.put("runtime", runtime);
        MappingMetadata indexMappingMetadata = new MappingMetadata("_doc", indexMappings);

        ImmutableOpenMap.Builder<String, MappingMetadata> indexMappingsMap = ImmutableOpenMap.builder();
        indexMappingsMap.put("_doc", indexMappingMetadata);
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", indexMappingsMap.build());

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        ImmutableOpenMap<String, MappingMetadata> mergedMappings = MappingsMerger.mergeMappings(
            newSourceWithExcludes("field_1", "runtime_field_2"),
            getMappingsResponse
        );

        assertThat(mergedMappings.size(), equalTo(1));
        assertThat(mergedMappings.containsKey("_doc"), is(true));
        Map<String, Object> mappingsAsMap = mergedMappings.valuesIt().next().getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> propertyMappings = (Map<String, Object>) mappingsAsMap.get("properties");
        assertThat(propertyMappings.keySet(), containsInAnyOrder("field_2"));

        @SuppressWarnings("unchecked")
        Map<String, Object> runtimeMappings = (Map<String, Object>) mappingsAsMap.get("runtime");
        assertThat(runtimeMappings.keySet(), containsInAnyOrder("runtime_field_1"));
    }

    private static DataFrameAnalyticsSource newSource() {
        return new DataFrameAnalyticsSource(new String[] { "index" }, null, null, null);
    }

    private static DataFrameAnalyticsSource newSourceWithExcludes(String... excludes) {
        return new DataFrameAnalyticsSource(new String[] { "index" }, null, new FetchSourceContext(true, null, excludes), null);
    }
}
