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
package org.elasticsearch.index.mapper.dynamic;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DynamicMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testDynamicTrue() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "true")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));
    }

    @Test
    public void testDynamicFalse() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .bytes());

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());
    }


    @Test
    public void testDynamicStrict() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        try {
            defaultMapper.parse("type", "1", jsonBuilder()
                    .startObject()
                    .field("field1", "value1")
                    .field("field2", "value2")
                    .bytes());
            fail();
        } catch (StrictDynamicMappingException e) {
            // all is well
        }

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field1", "value1")
                    .field("field2", (String) null)
                    .bytes());
            fail();
        } catch (StrictDynamicMappingException e) {
            // all is well
        }
    }

    @Test
    public void testDynamicFalseWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "false")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", jsonBuilder()
                .startObject().startObject("obj1")
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("obj1.field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("obj1.field2"), nullValue());
    }

    @Test
    public void testDynamicStrictWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("obj1").startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .endObject().endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        try {
            defaultMapper.parse("type", "1", jsonBuilder()
                    .startObject().startObject("obj1")
                    .field("field1", "value1")
                    .field("field2", "value2")
                    .endObject()
                    .bytes());
            fail();
        } catch (StrictDynamicMappingException e) {
            // all is well
        }
    }

    public void testDynamicMappingOnEmptyString() throws Exception {
        IndexService service = createIndex("test");
        client().prepareIndex("test", "type").setSource("empty_field", "").get();
        FieldMappers mappers = service.mapperService().indexName("empty_field");
        assertTrue(mappers != null && mappers.isEmpty() == false);
    }

    @Test
    public void testIndexingFailureDoesStillCreateType() throws IOException, InterruptedException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_default_")
                .field("dynamic", "strict")
                .endObject().endObject();

        IndexService indexService = createIndex("test", ImmutableSettings.EMPTY, "_default_", mapping);

        try {
            client().prepareIndex().setIndex("test").setType("type").setSource(jsonBuilder().startObject().field("test", "test").endObject()).get();
            fail();
        } catch (StrictDynamicMappingException e) {

        }
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(java.lang.Object input) {
                GetMappingsResponse currentMapping = client().admin().indices().prepareGetMappings("test").get();
                return currentMapping.getMappings().get("test").get("type") != null;
            }
        });

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNotNull(getMappingsResponse.getMappings().get("test").get("type"));
        DocumentMapper mapper = indexService.mapperService().documentMapper("type");
        assertNotNull(mapper);

    }

    @Test
    public void testTypeCreatedProperly() throws IOException, InterruptedException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_default_")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("test_string")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject().endObject();

        IndexService indexService = createIndex("test", ImmutableSettings.EMPTY, "_default_", mapping);

        try {
            client().prepareIndex().setIndex("test").setType("type").setSource(jsonBuilder().startObject().field("test", "test").endObject()).get();
            fail();
        } catch (StrictDynamicMappingException e) {

        }
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(java.lang.Object input) {
                GetMappingsResponse currentMapping = client().admin().indices().prepareGetMappings("test").get();
                return currentMapping.getMappings().get("test").get("type") != null;
            }
        });
        //type should be in mapping
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNotNull(getMappingsResponse.getMappings().get("test").get("type"));

        client().prepareIndex().setIndex("test").setType("type").setSource(jsonBuilder().startObject().field("test_string", "test").endObject()).get();
        client().admin().indices().prepareRefresh("test").get();
        assertThat(client().prepareSearch("test").get().getHits().getTotalHits(), equalTo(1l));

        DocumentMapper mapper = indexService.mapperService().documentMapper("type");
        assertNotNull(mapper);

        getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNotNull(getMappingsResponse.getMappings().get("test").get("type"));
    }

    @Test
    public void testFieldsCreatedWithPartialParsing() throws IOException, InterruptedException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("doc")
                .startObject("properties")
                .startObject("z")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject().endObject();

        IndexService indexService = createIndex("test", ImmutableSettings.EMPTY, "doc", mapping);
        boolean create = randomBoolean();
        if (create == false) {
            // we want to test sometimes create and sometimes index so sometimes add the document before and sometimes not
            client().prepareIndex().setIndex("test").setType("doc").setId("1").setSource(jsonBuilder().startObject().field("z", 0).endObject()).get();
        }
        try {
            IndexRequestBuilder indexRequest = client().prepareIndex().setIndex("test").setType("doc").setId("1").setSource(jsonBuilder().startObject().field("a", "string").field("z", "string").endObject());
            indexRequest.setCreate(create);
            indexRequest.get();
            fail();
        } catch (MapperParsingException e) {
            // this should fail because the field z is of type long
        }
        //type should be in mapping
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNotNull(getMappingsResponse.getMappings().get("test").get("doc"));

        client().prepareIndex().setIndex("test").setType("doc").setId("1").setSource(jsonBuilder().startObject().field("a", "string").field("z", 0).endObject()).get();
        client().admin().indices().prepareRefresh("test").get();
        assertThat(client().prepareSearch("test").get().getHits().getTotalHits(), equalTo(1l));

        // both fields should be in local mapper
        DocumentMapper mapper = indexService.mapperService().documentMapper("doc");
        assertNotNull(mapper.mappers().name("a"));
        assertNotNull(mapper.mappers().name("z"));

        // we have to wait here because the cluster state might not be immediately updated
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(java.lang.Object input) {
                GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
                return getMappingsResponse.getMappings().get("test").get("doc") != null;
            }
        }));
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(java.lang.Object input) {
                // both fields should be in the cluster state
                GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
                assertNotNull(getMappingsResponse.getMappings().get("test").get("doc"));
                Map<String, Object> mappings = null;
                try {
                    mappings = getMappingsResponse.getMappings().get("test").get("doc").getSourceAsMap();
                } catch (IOException e) {
                    fail("IOException when calling getSourceAsMap()" + e.getMessage());
                }
                return ((LinkedHashMap) mappings.get("properties")).get("a") != null && ((LinkedHashMap) mappings.get("properties")).get("z") != null;
            }
        }));
    }
}
