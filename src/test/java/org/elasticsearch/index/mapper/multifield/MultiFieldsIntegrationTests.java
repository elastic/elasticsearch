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

package org.elasticsearch.index.mapper.multifield;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 */
public class MultiFieldsIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testMultiFields() throws Exception {
        assertAcked(
            client().admin().indices().prepareCreate("my-index")
                .addMapping("my-type", createTypeSource())
        );

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        MappingMetaData mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
        assertThat(((Map) XContentMapValues.extractValue("properties.title.fields", mappingSource)).size(), equalTo(1));

        client().prepareIndex("my-index", "my-type", "1")
                .setSource("title", "Multi fields")
                .setRefresh(true)
                .get();

        SearchResponse searchResponse = client().prepareSearch("my-index")
                .setQuery(QueryBuilders.matchQuery("title", "multi"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch("my-index")
                .setQuery(QueryBuilders.matchQuery("title.not_analyzed", "Multi fields"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertAcked(
                client().admin().indices().preparePutMapping("my-index").setType("my-type")
                        .setSource(createPutMappingSource())
                        .setIgnoreConflicts(true) // If updated with multi-field type, we need to ignore failures.
        );

        getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        mappingSource = mappingMetaData.sourceAsMap();
        assertThat(((Map) XContentMapValues.extractValue("properties.title", mappingSource)).size(), equalTo(2));
        assertThat(((Map) XContentMapValues.extractValue("properties.title.fields", mappingSource)).size(), equalTo(2));

        searchResponse = client().prepareSearch("my-index")
                .setQuery(QueryBuilders.matchQuery("title.uncased", "multi"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
        searchResponse = client().prepareSearch("my-index")
                .setQuery(QueryBuilders.matchQuery("title.uncased", "Multi"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
        client().prepareIndex("my-index", "my-type", "1")
                .setSource("title", "Multi fields")
                .setRefresh(true)
                .get();

        searchResponse = client().prepareSearch("my-index")
                .setQuery(QueryBuilders.matchQuery("title.uncased", "Multi"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    private XContentBuilder createTypeSource() throws IOException {
        if (randomBoolean()) {
            return XContentFactory.jsonBuilder().startObject().startObject("my-type")
                    .startObject("properties")
                    .startObject("title")
                    .field("type", "string")
                    .startObject("fields")
                    .startObject("not_analyzed")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject().endObject();
        } else {
            return XContentFactory.jsonBuilder().startObject().startObject("my-type")
                    .startObject("properties")
                    .startObject("title")
                    .field("type", "multi_field")
                    .startObject("fields")
                    .startObject("title")
                    .field("type", "string")
                    .endObject()
                    .startObject("not_analyzed")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject().endObject();
        }
    }

    private XContentBuilder createPutMappingSource() throws IOException {
        if (randomBoolean()) {
            return XContentFactory.jsonBuilder().startObject().startObject("my-type")
                    .startObject("properties")
                    .startObject("title")
                    .field("type", "string")
                    .startObject("fields")
                    .startObject("uncased")
                    .field("type", "string")
                    .field("analyzer", "whitespace")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject().endObject();
        } else {
            return XContentFactory.jsonBuilder().startObject().startObject("my-type")
                    .startObject("properties")
                    .startObject("title")
                    .field("type", "multi_field")
                    .startObject("fields")
                    .startObject("uncased")
                    .field("type", "string")
                    .field("analyzer", "whitespace")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject().endObject();
        }
    }

}
