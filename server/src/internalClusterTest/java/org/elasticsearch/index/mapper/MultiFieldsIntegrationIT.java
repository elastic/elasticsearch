/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MultiFieldsIntegrationIT extends ESIntegTestCase {
    @SuppressWarnings("unchecked")
    public void testMultiFields() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("my-index").setMapping(createTypeSource()));

        GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "my-index").get();
        MappingMetadata mappingMetadata = getMappingsResponse.mappings().get("my-index");
        assertThat(mappingMetadata, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
        Map<String, Object> titleFields = ((Map<String, Object>) XContentMapValues.extractValue("properties.title.fields", mappingSource));
        assertThat(titleFields.size(), equalTo(1));
        assertThat(titleFields.get("not_analyzed"), notNullValue());
        assertThat(((Map<String, Object>) titleFields.get("not_analyzed")).get("type").toString(), equalTo("keyword"));

        prepareIndex("my-index").setId("1").setSource("title", "Multi fields").setRefreshPolicy(IMMEDIATE).get();

        assertHitCount(prepareSearch("my-index").setQuery(matchQuery("title", "multi")), 1);
        assertHitCount(prepareSearch("my-index").setQuery(matchQuery("title.not_analyzed", "Multi fields")), 1);

        assertAcked(indicesAdmin().preparePutMapping("my-index").setSource(createPutMappingSource()));

        getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "my-index").get();
        mappingMetadata = getMappingsResponse.mappings().get("my-index");
        assertThat(mappingMetadata, not(nullValue()));
        mappingSource = mappingMetadata.sourceAsMap();
        assertThat(((Map<String, Object>) XContentMapValues.extractValue("properties.title", mappingSource)).size(), equalTo(2));
        titleFields = ((Map<String, Object>) XContentMapValues.extractValue("properties.title.fields", mappingSource));
        assertThat(titleFields.size(), equalTo(2));
        assertThat(titleFields.get("not_analyzed"), notNullValue());
        assertThat(((Map<String, Object>) titleFields.get("not_analyzed")).get("type").toString(), equalTo("keyword"));
        assertThat(titleFields.get("uncased"), notNullValue());
        assertThat(((Map<String, Object>) titleFields.get("uncased")).get("analyzer").toString(), equalTo("whitespace"));

        prepareIndex("my-index").setId("1").setSource("title", "Multi fields").setRefreshPolicy(IMMEDIATE).get();

        assertHitCount(prepareSearch("my-index").setQuery(matchQuery("title.uncased", "Multi")), 1);
    }

    @SuppressWarnings("unchecked")
    public void testGeoPointMultiField() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("my-index").setMapping(createMappingSource("geo_point")));

        GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "my-index").get();
        MappingMetadata mappingMetadata = getMappingsResponse.mappings().get("my-index");
        assertThat(mappingMetadata, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
        Map<String, Object> aField = ((Map<String, Object>) XContentMapValues.extractValue("properties.a", mappingSource));
        logger.info("Keys: {}", aField.keySet());
        assertThat(aField.size(), equalTo(2));
        assertThat(aField.get("type").toString(), equalTo("geo_point"));
        assertThat(aField.get("fields"), notNullValue());

        Map<String, Object> bField = ((Map<String, Object>) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(1));
        assertThat(bField.get("type").toString(), equalTo("keyword"));

        GeoPoint point = new GeoPoint(51, 19);
        prepareIndex("my-index").setId("1").setSource("a", point.toString()).setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(
            1L,
            prepareSearch("my-index").setSize(0)
                .setQuery(constantScoreQuery(geoDistanceQuery("a").point(51, 19).distance(50, DistanceUnit.KILOMETERS))),
            prepareSearch("my-index").setSize(0).setQuery(matchQuery("a.b", point.geohash()))
        );
    }

    @SuppressWarnings("unchecked")
    public void testCompletionMultiField() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("my-index").setMapping(createMappingSource("completion")));

        GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "my-index").get();
        MappingMetadata mappingMetadata = getMappingsResponse.mappings().get("my-index");
        assertThat(mappingMetadata, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
        Map<String, Object> aField = ((Map<String, Object>) XContentMapValues.extractValue("properties.a", mappingSource));
        assertThat(aField.size(), equalTo(6));
        assertThat(aField.get("type").toString(), equalTo("completion"));
        assertThat(aField.get("fields"), notNullValue());

        Map<String, Object> bField = ((Map<String, Object>) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(1));
        assertThat(bField.get("type").toString(), equalTo("keyword"));

        prepareIndex("my-index").setId("1").setSource("a", "complete me").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(prepareSearch("my-index").setSize(0).setQuery(matchQuery("a.b", "complete me")), 1L);
    }

    @SuppressWarnings("unchecked")
    public void testIpMultiField() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("my-index").setMapping(createMappingSource("ip")));

        GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "my-index").get();
        MappingMetadata mappingMetadata = getMappingsResponse.mappings().get("my-index");
        assertThat(mappingMetadata, not(nullValue()));
        Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
        Map<String, Object> aField = ((Map<String, Object>) XContentMapValues.extractValue("properties.a", mappingSource));
        assertThat(aField.size(), equalTo(2));
        assertThat(aField.get("type").toString(), equalTo("ip"));
        assertThat(aField.get("fields"), notNullValue());

        Map<String, Object> bField = ((Map<String, Object>) XContentMapValues.extractValue("properties.a.fields.b", mappingSource));
        assertThat(bField.size(), equalTo(1));
        assertThat(bField.get("type").toString(), equalTo("keyword"));

        prepareIndex("my-index").setId("1").setSource("a", "127.0.0.1").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(prepareSearch("my-index").setSize(0).setQuery(matchQuery("a.b", "127.0.0.1")), 1L);
    }

    private XContentBuilder createMappingSource(String fieldType) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("a")
            .field("type", fieldType)
            .startObject("fields")
            .startObject("b")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    private XContentBuilder createTypeSource() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("title")
            .field("type", "text")
            .startObject("fields")
            .startObject("not_analyzed")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    private XContentBuilder createPutMappingSource() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("title")
            .field("type", "text")
            .startObject("fields")
            .startObject("uncased")
            .field("type", "text")
            .field("analyzer", "whitespace")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

}
