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

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiPredicate;

import static org.elasticsearch.cluster.metadata.MetaDataTests.assertLeafs;
import static org.elasticsearch.cluster.metadata.MetaDataTests.assertMultiField;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class FieldFilterMapperPluginTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(FieldFilterPlugin.class);
    }

    @Before
    public void putMappings() {
        assertAcked(client().admin().indices().prepareCreate("index1"));
        assertAcked(client().admin().indices().prepareCreate("filtered"));
        assertAcked(client().admin().indices().preparePutMapping("index1", "filtered")
                .setType("doc").setSource(TEST_ITEM, XContentType.JSON));
    }

    public void testGetMappings() {
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings().get();
        assertExpectedMappings(getMappingsResponse.mappings());
    }

    public void testGetIndex() {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex()
                .setFeatures(GetIndexRequest.Feature.MAPPINGS).get();
        assertExpectedMappings(getIndexResponse.mappings());
    }

    private void assertExpectedMappings(ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings) {
        assertEquals(2, mappings.size());
        assertNotFiltered(mappings.get("index1"));
        ImmutableOpenMap<String, MappingMetaData> filtered = mappings.get("filtered");
        assertFiltered(filtered);

        //try and see if the returned filtered mappings are still valid mappings by submitting them and retrieving them back
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("doc", filtered.get("doc").getSourceAsMap()));
        GetMappingsResponse testMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertEquals(1, testMappingsResponse.getMappings().size());
        //the mappings are returned unfiltered for this index, yet they are the same as the previous ones that were returned filtered
        assertFiltered(testMappingsResponse.getMappings().get("test"));
    }

    @SuppressWarnings("unchecked")
    private static void assertFiltered(ImmutableOpenMap<String, MappingMetaData> mappings) {
        assertEquals(1, mappings.size());
        MappingMetaData mappingMetaData = mappings.get("doc");
        assertNotNull(mappingMetaData);
        Map<String, Object> sourceAsMap = mappingMetaData.getSourceAsMap();
        assertEquals(1, sourceAsMap.size());
        Map<String, Object> typeProperties = (Map<String, Object>)sourceAsMap.get("properties");
        assertEquals(4, typeProperties.size());

        Map<String, Object> name = (Map<String, Object>)typeProperties.get("name");
        assertEquals(1, name.size());
        Map<String, Object> nameProperties = (Map<String, Object>)name.get("properties");
        assertEquals(1, nameProperties.size());
        assertLeafs(nameProperties, "last_visible");

        assertLeafs(typeProperties, "age_visible");

        Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
        assertNotNull(address);
        assertEquals(1, address.size());
        Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
        assertNotNull(addressProperties);
        assertEquals(1, addressProperties.size());
        assertLeafs(addressProperties, "area_visible");

        Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());
        assertEquals("nested", properties.get("type"));
        Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
        assertNotNull(propertiesProperties);
        assertEquals(2, propertiesProperties.size());
        assertLeafs(propertiesProperties, "key_visible");

        Map<String, Object> value = (Map<String, Object>) propertiesProperties.get("value");
        assertNotNull(value);
        assertEquals(1, value.size());
        Map<String, Object> valueProperties = (Map<String, Object>) value.get("properties");
        assertNotNull(valueProperties);
        assertEquals(1, valueProperties.size());
        assertLeafs(valueProperties, "keyword_visible");
    }

    @SuppressWarnings("unchecked")
    private static void assertNotFiltered(ImmutableOpenMap<String, MappingMetaData> mappings) {
        assertEquals(1, mappings.size());
        MappingMetaData mappingMetaData = mappings.get("doc");
        assertNotNull(mappingMetaData);
        Map<String, Object> sourceAsMap = mappingMetaData.getSourceAsMap();
        assertEquals(1, sourceAsMap.size());
        Map<String, Object> typeProperties = (Map<String, Object>)sourceAsMap.get("properties");
        assertEquals(5, typeProperties.size());

        Map<String, Object> name = (Map<String, Object>)typeProperties.get("name");
        assertEquals(1, name.size());
        Map<String, Object> nameProperties = (Map<String, Object>)name.get("properties");
        assertEquals(2, nameProperties.size());
        assertLeafs(nameProperties, "first", "last_visible");

        assertLeafs(typeProperties, "birth", "age_visible");

        Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
        assertNotNull(address);
        assertEquals(1, address.size());
        Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
        assertNotNull(addressProperties);
        assertEquals(3, addressProperties.size());
        assertLeafs(addressProperties, "street", "location", "area_visible");

        Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());
        assertTrue(properties.containsKey("type"));
        Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
        assertNotNull(propertiesProperties);
        assertEquals(2, propertiesProperties.size());
        assertMultiField(propertiesProperties, "key_visible", "keyword");
        assertMultiField(propertiesProperties, "value", "keyword_visible");
    }

    public static class FieldFilterPlugin extends Plugin implements MapperPlugin {
        @Override
        public BiPredicate<String, String> getFieldFilter() {
            return (index, field) -> index.equals("filtered") == false || field.endsWith("visible");
        }
    }

    private static final String TEST_ITEM = "{\n" +
            "  \"doc\": {\n" +
            "      \"properties\": {\n" +
            "        \"name\": {\n" +
            "          \"properties\": {\n" +
            "            \"first\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            },\n" +
            "            \"last_visible\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        \"birth\": {\n" +
            "          \"type\": \"date\"\n" +
            "        },\n" +
            "        \"age_visible\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        },\n" +
            "        \"address\": {\n" +
            "          \"type\": \"object\",\n" +
            "          \"properties\": {\n" +
            "            \"street\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            },\n" +
            "            \"location\": {\n" +
            "              \"type\": \"geo_point\"\n" +
            "            },\n" +
            "            \"area_visible\": {\n" +
            "              \"type\": \"geo_shape\",  \n" +
            "              \"tree\": \"quadtree\",\n" +
            "              \"precision\": \"1m\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        \"properties\": {\n" +
            "          \"type\": \"nested\",\n" +
            "          \"properties\": {\n" +
            "            \"key_visible\" : {\n" +
            "              \"type\": \"text\",\n" +
            "              \"fields\": {\n" +
            "                \"keyword\" : {\n" +
            "                  \"type\" : \"keyword\"\n" +
            "                }\n" +
            "              }\n" +
            "            },\n" +
            "            \"value\" : {\n" +
            "              \"type\": \"text\",\n" +
            "              \"fields\": {\n" +
            "                \"keyword_visible\" : {\n" +
            "                  \"type\" : \"keyword\"\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
}
