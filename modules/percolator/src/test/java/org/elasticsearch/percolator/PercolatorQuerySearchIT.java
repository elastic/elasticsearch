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
package org.elasticsearch.percolator;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.GeoPlugin;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNotQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;

public class PercolatorQuerySearchIT extends ESIntegTestCase {

    @Override
    protected boolean addMockGeoShapeFieldMapper() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(PercolatorPlugin.class, GeoPlugin.class);
    }

    public void testPercolatorQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=keyword", "field2", "type=keyword", "query", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", matchAllQuery()).endObject())
                .get();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                    .field("id", "2")
                    .field("query", matchQuery("field1", "value")).endObject())
                .get();
        client().prepareIndex("test").setId("3")
                .setSource(jsonBuilder().startObject()
                    .field("id", "3")
                    .field("query", boolQuery()
                        .must(matchQuery("field1", "value"))
                        .must(matchQuery("field2", "value"))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject().endObject());
        logger.info("percolating empty doc");
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject());
        logger.info("percolating doc with 1 field");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValue(), equalTo(0));

        source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").field("field2", "value").endObject());
        logger.info("percolating doc with 2 fields");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(2).getFields().get("_percolator_document_slot").getValue(), equalTo(0));

        logger.info("percolating doc with 2 fields");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", Arrays.asList(
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").field("field2", "value").endObject())
                ), XContentType.JSON))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValues(), equalTo(Arrays.asList(0, 1)));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValues(), equalTo(Arrays.asList(0, 1)));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(2).getFields().get("_percolator_document_slot").getValues(), equalTo(Arrays.asList(1)));
    }

    public void testPercolatorRangeQueries() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("field1", "type=long", "field2", "type=double", "field3", "type=ip", "field4", "type=date",
                        "query", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(10).to(12)).endObject())
                .get();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(20).to(22)).endObject())
                .get();
        client().prepareIndex("test").setId("3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field1").from(10).to(12))
                        .must(rangeQuery("field1").from(12).to(14))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();
        client().prepareIndex("test").setId("4")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from(10).to(12)).endObject())
                .get();
        client().prepareIndex("test").setId("5")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from(20).to(22)).endObject())
                .get();
        client().prepareIndex("test").setId("6")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field2").from(10).to(12))
                        .must(rangeQuery("field2").from(12).to(14))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();
        client().prepareIndex("test").setId("7")
                .setSource(jsonBuilder().startObject()
                        .field("query", rangeQuery("field3").from("192.168.1.0").to("192.168.1.5"))
                        .endObject())
                .get();
        client().prepareIndex("test").setId("8")
                .setSource(jsonBuilder().startObject()
                        .field("query", rangeQuery("field3").from("192.168.1.20").to("192.168.1.30"))
                        .endObject())
                .get();
        client().prepareIndex("test").setId("9")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field3").from("192.168.1.0").to("192.168.1.5"))
                        .must(rangeQuery("field3").from("192.168.1.5").to("192.168.1.10"))
                ).endObject()).get();
        client().prepareIndex("test").setId("10")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field4").from("2010-01-01").to("2018-01-01"))
                        .must(rangeQuery("field4").from("2010-01-01").to("now"))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();

        // Test long range:
        BytesReference source = BytesReference.bytes(jsonBuilder().startObject().field("field1", 12).endObject());
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        logger.info("response={}", response);
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("1"));

        source = BytesReference.bytes(jsonBuilder().startObject().field("field1", 11).endObject());
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        // Test double range:
        source = BytesReference.bytes(jsonBuilder().startObject().field("field2", 12).endObject());
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("6"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("4"));

        source = BytesReference.bytes(jsonBuilder().startObject().field("field2", 11).endObject());
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("4"));

        // Test IP range:
        source = BytesReference.bytes(jsonBuilder().startObject().field("field3", "192.168.1.5").endObject());
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("9"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("7"));

        source = BytesReference.bytes(jsonBuilder().startObject().field("field3", "192.168.1.4").endObject());
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("7"));

        // Test date range:
        source = BytesReference.bytes(jsonBuilder().startObject().field("field4", "2016-05-15").endObject());
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("10"));
    }

    public void testPercolatorGeoQueries() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("id", "type=keyword",
                "field1", "type=geo_point", "field2", "type=geo_shape", "query", "type=percolator"));

        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject()
                .field("query", geoDistanceQuery("field1").point(52.18, 4.38).distance(50, DistanceUnit.KILOMETERS))
                .field("id", "1")
            .endObject()).get();

        client().prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject()
                .field("query", geoBoundingBoxQuery("field1").setCorners(52.3, 4.4, 52.1, 4.6))
                .field("id", "2")
            .endObject()).get();

        client().prepareIndex("test").setId("3")
            .setSource(jsonBuilder().startObject()
                .field("query",
                    geoPolygonQuery("field1", Arrays.asList(new GeoPoint(52.1, 4.4), new GeoPoint(52.3, 4.5), new GeoPoint(52.1, 4.6))))
                .field("id", "3")
            .endObject()).get();
        refresh();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
            .startObject("field1").field("lat", 52.20).field("lon", 4.51).endObject()
            .endObject());
        SearchResponse response = client().prepareSearch()
            .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
            .addSort("id", SortOrder.ASC)
            .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPercolatorQueryExistingDocument() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=keyword", "field2", "type=keyword", "query", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", matchAllQuery()).endObject())
                .get();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                    .field("id", "2")
                    .field("query", matchQuery("field1", "value")).endObject())
                .get();
        client().prepareIndex("test").setId("3")
                .setSource(jsonBuilder().startObject()
                    .field("id", "3")
                    .field("query", boolQuery()
                        .must(matchQuery("field1", "value"))
                        .must(matchQuery("field2", "value"))).endObject()).get();

        client().prepareIndex("test").setId("4").setSource("{\"id\": \"4\"}", XContentType.JSON).get();
        client().prepareIndex("test").setId("5").setSource(XContentType.JSON, "id", "5", "field1", "value").get();
        client().prepareIndex("test").setId("6").setSource(XContentType.JSON, "id", "6", "field1", "value", "field2", "value").get();
        client().admin().indices().prepareRefresh().get();

        logger.info("percolating empty doc");
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "test", "1", null, null, null))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        logger.info("percolating doc with 1 field");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "test", "5", null, null, null))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));

        logger.info("percolating doc with 2 fields");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "test", "6", null, null, null))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPercolatorQueryExistingDocumentSourceDisabled() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("_source", "enabled=false", "field1", "type=keyword", "query", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
            .get();

        client().prepareIndex("test").setId("2").setSource("{}", XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        logger.info("percolating empty doc with source disabled");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "test", "1", null, null, null))
                .get();
        });
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testPercolatorSpecificQueries()  throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=text", "field2", "type=text", "query", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", multiMatchQuery("quick brown fox", "field1", "field2")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)).endObject())
                .get();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                    .field("id", "2")
                    .field("query",
                        spanNearQuery(spanTermQuery("field1", "quick"), 0)
                                .addClause(spanTermQuery("field1", "brown"))
                                .addClause(spanTermQuery("field1", "fox"))
                                .inOrder(true)
                ).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        client().prepareIndex("test").setId("3")
                .setSource(jsonBuilder().startObject()
                    .field("id", "3")
                    .field("query",
                        spanNotQuery(
                                spanNearQuery(spanTermQuery("field1", "quick"), 0)
                                        .addClause(spanTermQuery("field1", "brown"))
                                        .addClause(spanTermQuery("field1", "fox"))
                                        .inOrder(true),
                                spanNearQuery(spanTermQuery("field1", "the"), 0)
                                        .addClause(spanTermQuery("field1", "lazy"))
                                        .addClause(spanTermQuery("field1", "dog"))
                                        .inOrder(true)).dist(2)
                ).endObject())
                .get();

        // doesn't match
        client().prepareIndex("test").setId("4")
                .setSource(jsonBuilder().startObject()
                    .field("id", "4")
                    .field("query",
                        spanNotQuery(
                                spanNearQuery(spanTermQuery("field1", "quick"), 0)
                                        .addClause(spanTermQuery("field1", "brown"))
                                        .addClause(spanTermQuery("field1", "fox"))
                                        .inOrder(true),
                                spanNearQuery(spanTermQuery("field1", "the"), 0)
                                        .addClause(spanTermQuery("field1", "lazy"))
                                        .addClause(spanTermQuery("field1", "dog"))
                                        .inOrder(true)).dist(3)
                ).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
                .field("field1", "the quick brown fox jumps over the lazy dog")
                .field("field2", "the quick brown fox falls down into the well")
                .endObject());
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getScore(), equalTo(Float.NaN));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getScore(), equalTo(Float.NaN));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(2).getScore(), equalTo(Float.NaN));
    }

    public void testPercolatorQueryWithHighlighting() throws Exception {
        StringBuilder fieldMapping = new StringBuilder("type=text")
                .append(",store=").append(randomBoolean());
        if (randomBoolean()) {
            fieldMapping.append(",term_vector=with_positions_offsets");
        } else if (randomBoolean()) {
            fieldMapping.append(",index_options=offsets");
        }
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", fieldMapping.toString(), "query", "type=percolator")
        );
        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", matchQuery("field1", "brown fox")).endObject())
                .execute().actionGet();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                    .field("id", "2")
                    .field("query", matchQuery("field1", "lazy dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test").setId("3")
                .setSource(jsonBuilder().startObject()
                    .field("id", "3")
                    .field("query", termQuery("field1", "jumps")).endObject())
                .execute().actionGet();
        client().prepareIndex("test").setId("4")
                .setSource(jsonBuilder().startObject()
                    .field("id", "4")
                    .field("query", termQuery("field1", "dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test").setId("5")
                .setSource(jsonBuilder().startObject()
                    .field("id", "5")
                    .field("query", termQuery("field1", "fox")).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().get();

        BytesReference document = BytesReference.bytes(jsonBuilder().startObject()
                .field("field1", "The quick brown fox jumps over the lazy dog")
                .endObject());
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", document, XContentType.JSON))
                .highlighter(new HighlightBuilder().field("field1"))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, 5);

        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(searchResponse.getHits().getAt(1).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(2).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(searchResponse.getHits().getAt(3).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));

        BytesReference document1 = BytesReference.bytes(jsonBuilder().startObject()
            .field("field1", "The quick brown fox jumps")
            .endObject());
        BytesReference document2 = BytesReference.bytes(jsonBuilder().startObject()
            .field("field1", "over the lazy dog")
            .endObject());
        searchResponse = client().prepareSearch()
            .setQuery(boolQuery()
                .should(new PercolateQueryBuilder("query", document1, XContentType.JSON).setName("query1"))
                .should(new PercolateQueryBuilder("query", document2, XContentType.JSON).setName("query2"))
            )
            .highlighter(new HighlightBuilder().field("field1"))
            .addSort("id", SortOrder.ASC)
            .get();
        logger.info("searchResponse={}", searchResponse);
        assertHitCount(searchResponse, 5);

        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("query1_field1").fragments()[0].string(),
            equalTo("The quick <em>brown</em> <em>fox</em> jumps"));
        assertThat(searchResponse.getHits().getAt(1).getHighlightFields().get("query2_field1").fragments()[0].string(),
            equalTo("over the <em>lazy</em> <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(2).getHighlightFields().get("query1_field1").fragments()[0].string(),
            equalTo("The quick brown fox <em>jumps</em>"));
        assertThat(searchResponse.getHits().getAt(3).getHighlightFields().get("query2_field1").fragments()[0].string(),
            equalTo("over the lazy <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("query1_field1").fragments()[0].string(),
            equalTo("The quick brown <em>fox</em> jumps"));

        searchResponse = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", Arrays.asList(
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "dog").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "fox").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "jumps").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "brown fox").endObject())
                ), XContentType.JSON))
                .highlighter(new HighlightBuilder().field("field1"))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, 5);
        assertThat(searchResponse.getHits().getAt(0).getFields().get("_percolator_document_slot").getValues(),
                equalTo(Arrays.asList(1, 3)));
        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("1_field1").fragments()[0].string(),
                equalTo("<em>fox</em>"));
        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("3_field1").fragments()[0].string(),
                equalTo("<em>brown</em> <em>fox</em>"));
        assertThat(searchResponse.getHits().getAt(1).getFields().get("_percolator_document_slot").getValues(),
                equalTo(Collections.singletonList(0)));
        assertThat(searchResponse.getHits().getAt(1).getHighlightFields().get("0_field1").fragments()[0].string(),
                equalTo("<em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(2).getFields().get("_percolator_document_slot").getValues(),
                equalTo(Collections.singletonList(2)));
        assertThat(searchResponse.getHits().getAt(2).getHighlightFields().get("2_field1").fragments()[0].string(),
                equalTo("<em>jumps</em>"));
        assertThat(searchResponse.getHits().getAt(3).getFields().get("_percolator_document_slot").getValues(),
                equalTo(Collections.singletonList(0)));
        assertThat(searchResponse.getHits().getAt(3).getHighlightFields().get("0_field1").fragments()[0].string(),
                equalTo("<em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(4).getFields().get("_percolator_document_slot").getValues(),
                equalTo(Arrays.asList(1, 3)));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("1_field1").fragments()[0].string(),
                equalTo("<em>fox</em>"));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("3_field1").fragments()[0].string(),
                equalTo("brown <em>fox</em>"));

        searchResponse = client().prepareSearch()
            .setQuery(boolQuery()
                .should(new PercolateQueryBuilder("query", Arrays.asList(
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "dog").endObject()),
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "fox").endObject())
                ), XContentType.JSON).setName("query1"))
                .should(new PercolateQueryBuilder("query", Arrays.asList(
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "jumps").endObject()),
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "brown fox").endObject())
                ), XContentType.JSON).setName("query2"))
            )
            .highlighter(new HighlightBuilder().field("field1"))
            .addSort("id", SortOrder.ASC)
            .get();
        logger.info("searchResponse={}", searchResponse);
        assertHitCount(searchResponse, 5);
        assertThat(searchResponse.getHits().getAt(0).getFields().get("_percolator_document_slot_query1").getValues(),
            equalTo(Collections.singletonList(1)));
        assertThat(searchResponse.getHits().getAt(0).getFields().get("_percolator_document_slot_query2").getValues(),
            equalTo(Collections.singletonList(1)));
        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("query1_1_field1").fragments()[0].string(),
            equalTo("<em>fox</em>"));
        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("query2_1_field1").fragments()[0].string(),
            equalTo("<em>brown</em> <em>fox</em>"));

        assertThat(searchResponse.getHits().getAt(1).getFields().get("_percolator_document_slot_query1").getValues(),
            equalTo(Collections.singletonList(0)));
        assertThat(searchResponse.getHits().getAt(1).getHighlightFields().get("query1_0_field1").fragments()[0].string(),
            equalTo("<em>dog</em>"));

        assertThat(searchResponse.getHits().getAt(2).getFields().get("_percolator_document_slot_query2").getValues(),
            equalTo(Collections.singletonList(0)));
        assertThat(searchResponse.getHits().getAt(2).getHighlightFields().get("query2_0_field1").fragments()[0].string(),
            equalTo("<em>jumps</em>"));

        assertThat(searchResponse.getHits().getAt(3).getFields().get("_percolator_document_slot_query1").getValues(),
            equalTo(Collections.singletonList(0)));
        assertThat(searchResponse.getHits().getAt(3).getHighlightFields().get("query1_0_field1").fragments()[0].string(),
            equalTo("<em>dog</em>"));

        assertThat(searchResponse.getHits().getAt(4).getFields().get("_percolator_document_slot_query1").getValues(),
            equalTo(Collections.singletonList(1)));
        assertThat(searchResponse.getHits().getAt(4).getFields().get("_percolator_document_slot_query2").getValues(),
            equalTo(Collections.singletonList(1)));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("query1_1_field1").fragments()[0].string(),
            equalTo("<em>fox</em>"));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("query2_1_field1").fragments()[0].string(),
            equalTo("brown <em>fox</em>"));
    }

    public void testTakePositionOffsetGapIntoAccount() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("field", "type=text,position_increment_gap=5", "query", "type=percolator")
        );
        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject().field("query",
                        new MatchPhraseQueryBuilder("field", "brown fox").slop(4)).endObject())
                .get();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject().field("query",
                        new MatchPhraseQueryBuilder("field", "brown fox").slop(5)).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse response = client().prepareSearch().setQuery(
                new PercolateQueryBuilder("query", new BytesArray("{\"field\" : [\"brown\", \"fox\"]}"), XContentType.JSON)
        ).get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
    }


    public void testManyPercolatorFields() throws Exception {
        String queryFieldName = randomAlphaOfLength(8);
        assertAcked(client().admin().indices().prepareCreate("test1")
                .setMapping(queryFieldName, "type=percolator", "field", "type=keyword")
        );
        assertAcked(client().admin().indices().prepareCreate("test2")
            .setMapping(queryFieldName, "type=percolator", "second_query_field", "type=percolator", "field", "type=keyword")
        );
        assertAcked(client().admin().indices().prepareCreate("test3")
            .setMapping(jsonBuilder().startObject().startObject("_doc").startObject("properties")
                .startObject("field")
                .field("type", "keyword")
                .endObject()
                .startObject("object_field")
                .field("type", "object")
                .startObject("properties")
                .startObject(queryFieldName)
                .field("type", "percolator")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject().endObject())
        );
    }

    public void testWithMultiplePercolatorFields() throws Exception {
        String queryFieldName = randomAlphaOfLength(8);
        assertAcked(client().admin().indices().prepareCreate("test1")
                .setMapping(queryFieldName, "type=percolator", "field", "type=keyword"));
        assertAcked(client().admin().indices().prepareCreate("test2")
                .setMapping(jsonBuilder().startObject().startObject("_doc").startObject("properties")
                        .startObject("field")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("object_field")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject(queryFieldName)
                        .field("type", "percolator")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject().endObject())
        );

        // Acceptable:
        client().prepareIndex("test1").setId("1")
                .setSource(jsonBuilder().startObject().field(queryFieldName, matchQuery("field", "value")).endObject())
                .get();
        client().prepareIndex("test2").setId("1")
                .setSource(jsonBuilder().startObject().startObject("object_field")
                        .field(queryFieldName, matchQuery("field", "value"))
                        .endObject().endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject().field("field", "value").endObject());
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder(queryFieldName, source, XContentType.JSON))
                .setIndices("test1")
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getIndex(), equalTo("test1"));

        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("object_field." + queryFieldName, source, XContentType.JSON))
                .setIndices("test2")
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getIndex(), equalTo("test2"));

        // Unacceptable:
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            client().prepareIndex("test2").setId("1")
                    .setSource(jsonBuilder().startObject().startArray("object_field")
                            .startObject().field(queryFieldName, matchQuery("field", "value")).endObject()
                            .startObject().field(queryFieldName, matchQuery("field", "value")).endObject()
                            .endArray().endObject())
                    .get();
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("a document can only contain one percolator query"));
    }

    public void testPercolateQueryWithNestedDocuments() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("query").field("type", "percolator").endObject()
                .startObject("companyname").field("type", "text").endObject().startObject("employee").field("type", "nested")
                .startObject("properties").startObject("name").field("type", "text").endObject().endObject().endObject().endObject()
                .endObject();
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping(mapping)
        );
        client().prepareIndex("test").setId("q1").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee",
                        QueryBuilders.matchQuery("employee.name", "virginia potts").operator(Operator.AND), ScoreMode.Avg)
                ).endObject())
                .get();
        // this query should never match as it doesn't use nested query:
        client().prepareIndex("test").setId("q2").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.matchQuery("employee.name", "virginia")).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query",
                        BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject().field("companyname", "stark")
                                .startArray("employee")
                                    .startObject().field("name", "virginia potts").endObject()
                                    .startObject().field("name", "tony stark").endObject()
                                .endArray()
                            .endObject()), XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("q1"));

        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query",
                        BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject().field("companyname", "notstark")
                                .startArray("employee")
                                    .startObject().field("name", "virginia stark").endObject()
                                    .startObject().field("name", "tony stark").endObject()
                                .endArray()
                            .endObject()), XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
        assertHitCount(response, 0);

        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query",
                        BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("companyname", "notstark").endObject()),
                    XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
        assertHitCount(response, 0);

        response = client().prepareSearch()
            .setQuery(new PercolateQueryBuilder("query", Arrays.asList(
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().field("companyname", "stark")
                    .startArray("employee")
                    .startObject().field("name", "virginia potts").endObject()
                    .startObject().field("name", "tony stark").endObject()
                    .endArray()
                    .endObject()),
                BytesReference.bytes(XContentFactory.jsonBuilder()
                    .startObject().field("companyname", "stark")
                    .startArray("employee")
                    .startObject().field("name", "peter parker").endObject()
                    .startObject().field("name", "virginia potts").endObject()
                    .endArray()
                    .endObject())
            ), XContentType.JSON))
            .addSort("_doc", SortOrder.ASC)
            .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("q1"));
        assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValues(), equalTo(Arrays.asList(0, 1)));
    }

    public void testPercolatorQueryViaMultiSearch() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("field1", "type=text", "query", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
            .execute().actionGet();
        client().prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
            .execute().actionGet();
        client().prepareIndex("test").setId("3")
            .setSource(jsonBuilder().startObject().field("query", boolQuery()
                .must(matchQuery("field1", "b"))
                .must(matchQuery("field1", "c"))
            ).endObject())
            .execute().actionGet();
        client().prepareIndex("test").setId("4")
            .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
            .execute().actionGet();
        client().prepareIndex("test").setId("5")
            .setSource(jsonBuilder().startObject().field("field1", "c").endObject())
            .execute().actionGet();
        client().admin().indices().prepareRefresh().get();

        MultiSearchResponse response = client().prepareMultiSearch()
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query",
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "b").endObject()), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query",
                    BytesReference.bytes(yamlBuilder().startObject().field("field1", "c").endObject()), XContentType.YAML)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query",
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "b c").endObject()), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query",
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "d").endObject()), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query", "test", "5", null, null, null)))
            .add(client().prepareSearch("test") // non existing doc, so error element
                .setQuery(new PercolateQueryBuilder("query", "test", "6", null, null, null)))
            .get();

        MultiSearchResponse.Item item = response.getResponses()[0];
        assertHitCount(item.getResponse(), 2L);
        assertSearchHits(item.getResponse(), "1", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[1];
        assertHitCount(item.getResponse(), 2L);
        assertSearchHits(item.getResponse(), "2", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[2];
        assertHitCount(item.getResponse(), 4L);
        assertSearchHits(item.getResponse(), "1", "2", "3", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[3];
        assertHitCount(item.getResponse(), 1L);
        assertSearchHits(item.getResponse(), "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[4];
        assertHitCount(item.getResponse(), 2L);
        assertSearchHits(item.getResponse(), "2", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[5];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.getFailureMessage(), notNullValue());
        assertThat(item.getFailureMessage(), containsString("[test/6] couldn't be found"));
    }

    public void testDisallowExpensiveQueries() throws IOException {
        try {
            assertAcked(client().admin().indices().prepareCreate("test")
                    .setMapping("id", "type=keyword", "field1", "type=keyword", "query", "type=percolator")
            );

            client().prepareIndex("test").setId("1")
                    .setSource(jsonBuilder().startObject()
                            .field("id", "1")
                            .field("query", matchQuery("field1", "value")).endObject())
                    .get();
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = false => success
            BytesReference source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject());
            SearchResponse response = client().prepareSearch()
                    .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));

            // Set search.allow_expensive_queries to "false" => assert failure
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            ElasticsearchException e = expectThrows(ElasticsearchException.class,
                    () -> client().prepareSearch()
                            .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                            .get());
            assertEquals("[percolate] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                    e.getCause().getMessage());

            // Set search.allow_expensive_queries setting to "true" ==> success
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            response = client().prepareSearch()
                    .setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON))
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    public void testWrappedWithConstantScore() throws Exception {

        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("d", "type=date", "q", "type=percolator")
        );

        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("q",
                boolQuery().must(rangeQuery("d").gt("now"))
            ).endObject())
            .execute().actionGet();

        client().prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("q",
                boolQuery().must(rangeQuery("d").lt("now"))
            ).endObject())
            .execute().actionGet();

        client().admin().indices().prepareRefresh().get();

        SearchResponse response = client().prepareSearch("test").setQuery(new PercolateQueryBuilder("q",
            BytesReference.bytes(jsonBuilder().startObject().field("d", "2020-02-01T15:00:00.000+11:00").endObject()),
            XContentType.JSON)).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch("test").setQuery(new PercolateQueryBuilder("q",
            BytesReference.bytes(jsonBuilder().startObject().field("d", "2020-02-01T15:00:00.000+11:00").endObject()),
            XContentType.JSON)).addSort("_doc", SortOrder.ASC).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        response = client().prepareSearch("test").setQuery(constantScoreQuery(new PercolateQueryBuilder("q",
            BytesReference.bytes(jsonBuilder().startObject().field("d", "2020-02-01T15:00:00.000+11:00").endObject()),
            XContentType.JSON))).get();
        assertEquals(1, response.getHits().getTotalHits().value);

    }
}
