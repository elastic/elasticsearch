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
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 */
public class MultiFieldsIntegrationIT extends ESIntegTestCase {

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
        Map titleFields = ((Map) XContentMapValues.extractValue("properties.title.fields", mappingSource));
        assertThat(titleFields.size(), equalTo(1));
        assertThat(titleFields.get("not_analyzed"), notNullValue());
        assertThat(((Map)titleFields.get("not_analyzed")).get("index").toString(), equalTo("not_analyzed"));

        client().prepareIndex("my-index", "my-type", "1")
                .setSource("title", "Multi fields")
                .setRefresh(true)
                .get();

        SearchResponse searchResponse = client().prepareSearch("my-index")
                .setQuery(matchQuery("title", "multi"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch("my-index")
                .setQuery(matchQuery("title.not_analyzed", "Multi fields"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertAcked(
                client().admin().indices().preparePutMapping("my-index").setType("my-type")
                        .setSource(createPutMappingSource())
        );

        getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        mappingSource = mappingMetaData.sourceAsMap();
        assertThat(((Map) XContentMapValues.extractValue("properties.title", mappingSource)).size(), equalTo(2));
        titleFields = ((Map) XContentMapValues.extractValue("properties.title.fields", mappingSource));
        assertThat(titleFields.size(), equalTo(2));
        assertThat(titleFields.get("not_analyzed"), notNullValue());
        assertThat(((Map)titleFields.get("not_analyzed")).get("index").toString(), equalTo("not_analyzed"));
        assertThat(titleFields.get("uncased"), notNullValue());
        assertThat(((Map)titleFields.get("uncased")).get("analyzer").toString(), equalTo("whitespace"));

        client().prepareIndex("my-index", "my-type", "1")
                .setSource("title", "Multi fields")
                .setRefresh(true)
                .get();

        searchResponse = client().prepareSearch("my-index")
                .setQuery(matchQuery("title.uncased", "Multi"))
                .get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testGeoPointMultiField() throws Exception {
        assertAcked(
                client().admin().indices().prepareCreate("my-index")
                        .addMapping("my-type", createMappingSource("geo_point"))
        );

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        MappingMetaData mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
        Map aField = ((Map) XContentMapValues.extractValue("properties.a", mappingSource));
        logger.info("Keys: " + aField.keySet());
        assertThat(aField.size(), equalTo(2));
        assertThat(aField.get("type").toString(), equalTo("geo_point"));
        assertThat(aField.get("fields"), notNullValue());

        Map bField = ((Map) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(2));
        assertThat(bField.get("type").toString(), equalTo("string"));
        assertThat(bField.get("index").toString(), equalTo("not_analyzed"));

        client().prepareIndex("my-index", "my-type", "1").setSource("a", "51,19").setRefresh(true).get();
        CountResponse countResponse = client().prepareCount("my-index")
                .setQuery(constantScoreQuery(geoDistanceQuery("a").point(51, 19).distance(50, DistanceUnit.KILOMETERS)))
                .get();
        assertThat(countResponse.getCount(), equalTo(1l));
        countResponse = client().prepareCount("my-index").setQuery(matchQuery("a.b", "51,19")).get();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testTokenCountMultiField() throws Exception {
        assertAcked(
                client().admin().indices().prepareCreate("my-index")
                        .addMapping("my-type", XContentFactory.jsonBuilder().startObject().startObject("my-type")
                                .startObject("properties")
                                .startObject("a")
                                .field("type", "token_count")
                                .field("analyzer", "simple")
                                .startObject("fields")
                                .startObject("b")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject().endObject())
        );

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        MappingMetaData mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
        Map aField = ((Map) XContentMapValues.extractValue("properties.a", mappingSource));
        assertThat(aField.size(), equalTo(3));
        assertThat(aField.get("type").toString(), equalTo("token_count"));
        assertThat(aField.get("fields"), notNullValue());

        Map bField = ((Map) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(2));
        assertThat(bField.get("type").toString(), equalTo("string"));
        assertThat(bField.get("index").toString(), equalTo("not_analyzed"));

        client().prepareIndex("my-index", "my-type", "1").setSource("a", "my tokens").setRefresh(true).get();
        CountResponse countResponse = client().prepareCount("my-index").setQuery(matchQuery("a.b", "my tokens")).get();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testCompletionMultiField() throws Exception {
        assertAcked(
                client().admin().indices().prepareCreate("my-index")
                        .addMapping("my-type", createMappingSource("completion"))
        );

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        MappingMetaData mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
        Map aField = ((Map) XContentMapValues.extractValue("properties.a", mappingSource));
        assertThat(aField.size(), equalTo(7));
        assertThat(aField.get("type").toString(), equalTo("completion"));
        assertThat(aField.get("fields"), notNullValue());

        Map bField = ((Map) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(2));
        assertThat(bField.get("type").toString(), equalTo("string"));
        assertThat(bField.get("index").toString(), equalTo("not_analyzed"));

        client().prepareIndex("my-index", "my-type", "1").setSource("a", "complete me").setRefresh(true).get();
        CountResponse countResponse = client().prepareCount("my-index").setQuery(matchQuery("a.b", "complete me")).get();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testIpMultiField() throws Exception {
        assertAcked(
                client().admin().indices().prepareCreate("my-index")
                        .addMapping("my-type", createMappingSource("ip"))
        );

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("my-index").get();
        MappingMetaData mappingMetaData = getMappingsResponse.mappings().get("my-index").get("my-type");
        assertThat(mappingMetaData, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
        Map aField = ((Map) XContentMapValues.extractValue("properties.a", mappingSource));
        assertThat(aField.size(), equalTo(2));
        assertThat(aField.get("type").toString(), equalTo("ip"));
        assertThat(aField.get("fields"), notNullValue());

        Map bField = ((Map) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(2));
        assertThat(bField.get("type").toString(), equalTo("string"));
        assertThat(bField.get("index").toString(), equalTo("not_analyzed"));

        client().prepareIndex("my-index", "my-type", "1").setSource("a", "127.0.0.1").setRefresh(true).get();
        CountResponse countResponse = client().prepareCount("my-index").setQuery(matchQuery("a.b", "127.0.0.1")).get();
        assertThat(countResponse.getCount(), equalTo(1l));
    }

    private XContentBuilder createMappingSource(String fieldType) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("my-type")
                .startObject("properties")
                .startObject("a")
                .field("type", fieldType)
                .startObject("fields")
                .startObject("b")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject().endObject();
    }

    private XContentBuilder createTypeSource() throws IOException {
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
    }

    private XContentBuilder createPutMappingSource() throws IOException {
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
    }

}
