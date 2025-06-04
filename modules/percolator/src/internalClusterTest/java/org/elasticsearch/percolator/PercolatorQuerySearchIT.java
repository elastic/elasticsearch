/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.percolator;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.combinedFieldsQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNotQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xcontent.XContentFactory.yamlBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;

public class PercolatorQuerySearchIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(PercolatorPlugin.class);
    }

    public void testPercolatorQuery() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=keyword", "field2", "type=keyword", "query", "type=percolator")
        );

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("id", "1").field("query", matchAllQuery()).endObject())
            .get();
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("id", "2").field("query", matchQuery("field1", "value")).endObject())
            .get();
        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "3")
                    .field("query", boolQuery().must(matchQuery("field1", "value")).must(matchQuery("field2", "value")))
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject().endObject());
        logger.info("percolating empty doc");
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        });

        source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject());
        logger.info("percolating doc with 1 field");
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 2);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
            }
        );

        source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").field("field2", "value").endObject());
        logger.info("percolating doc with 2 fields");
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 3);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(2).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
            }
        );
        logger.info("percolating doc with 2 fields");
        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder(
                    "query",
                    Arrays.asList(
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").field("field2", "value").endObject())
                    ),
                    XContentType.JSON
                )
            ).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 3);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(
                    response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Arrays.asList(0, 1))
                );
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(
                    response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Arrays.asList(0, 1))
                );
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(2).getFields().get("_percolator_document_slot").getValues(), equalTo(Arrays.asList(1)));
            }
        );
    }

    public void testPercolatorRangeQueries() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "field1",
                    "type=long",
                    "field2",
                    "type=double",
                    "field3",
                    "type=ip",
                    "field4",
                    "type=date",
                    "query",
                    "type=percolator"
                )
        );

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(10).to(12)).endObject())
            .get();
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(20).to(22)).endObject())
            .get();
        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", boolQuery().must(rangeQuery("field1").from(10).to(12)).must(rangeQuery("field1").from(12).to(14)))
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();
        prepareIndex("test").setId("4")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from(10).to(12)).endObject())
            .get();
        prepareIndex("test").setId("5")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from(20).to(22)).endObject())
            .get();
        prepareIndex("test").setId("6")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", boolQuery().must(rangeQuery("field2").from(10).to(12)).must(rangeQuery("field2").from(12).to(14)))
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();
        prepareIndex("test").setId("7")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field3").from("192.168.1.0").to("192.168.1.5")).endObject())
            .get();
        prepareIndex("test").setId("8")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field3").from("192.168.1.20").to("192.168.1.30")).endObject())
            .get();
        prepareIndex("test").setId("9")
            .setSource(
                jsonBuilder().startObject()
                    .field(
                        "query",
                        boolQuery().must(rangeQuery("field3").from("192.168.1.0").to("192.168.1.5"))
                            .must(rangeQuery("field3").from("192.168.1.5").to("192.168.1.10"))
                    )
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("10")
            .setSource(
                jsonBuilder().startObject()
                    .field(
                        "query",
                        boolQuery().must(rangeQuery("field4").from("2010-01-01").to("2018-01-01"))
                            .must(rangeQuery("field4").from("2010-01-01").to("now"))
                    )
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        // Test long range:
        BytesReference source = BytesReference.bytes(jsonBuilder().startObject().field("field1", 12).endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            logger.info("response={}", response);
            assertHitCount(response, 2);
            assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("1"));
        });

        source = BytesReference.bytes(jsonBuilder().startObject().field("field1", 11).endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        });
        // Test double range:
        source = BytesReference.bytes(jsonBuilder().startObject().field("field2", 12).endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 2);
            assertThat(response.getHits().getAt(0).getId(), equalTo("6"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("4"));
        });

        source = BytesReference.bytes(jsonBuilder().startObject().field("field2", 11).endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("4"));
        });

        // Test IP range:
        source = BytesReference.bytes(jsonBuilder().startObject().field("field3", "192.168.1.5").endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 2);
            assertThat(response.getHits().getAt(0).getId(), equalTo("9"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("7"));
        });

        source = BytesReference.bytes(jsonBuilder().startObject().field("field3", "192.168.1.4").endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("7"));
        });

        // Test date range:
        source = BytesReference.bytes(jsonBuilder().startObject().field("field4", "2016-05-15").endObject());
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("10"));
        });
    }

    public void testPercolatorGeoQueries() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test").setMapping("id", "type=keyword", "field1", "type=geo_point", "query", "type=percolator")
        );

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", geoDistanceQuery("field1").point(52.18, 4.38).distance(50, DistanceUnit.KILOMETERS))
                    .field("id", "1")
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", geoBoundingBoxQuery("field1").setCorners(52.3, 4.4, 52.1, 4.6))
                    .field("id", "2")
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field(
                        "query",
                        geoShapeQuery(
                            "field1",
                            new Polygon(new LinearRing(new double[] { 4.4, 4.5, 4.6, 4.4 }, new double[] { 52.1, 52.3, 52.1, 52.1 }))
                        )
                    )
                    .field("id", "3")
                    .endObject()
            )
            .get();
        refresh();

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject().startObject("field1").field("lat", 52.20).field("lon", 4.51).endObject().endObject()
        );
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 3);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
    }

    public void testPercolatorQueryExistingDocument() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=keyword", "field2", "type=keyword", "query", "type=percolator")
        );

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("id", "1").field("query", matchAllQuery()).endObject())
            .get();
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("id", "2").field("query", matchQuery("field1", "value")).endObject())
            .get();
        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "3")
                    .field("query", boolQuery().must(matchQuery("field1", "value")).must(matchQuery("field2", "value")))
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("4").setSource("{\"id\": \"4\"}", XContentType.JSON).get();
        prepareIndex("test").setId("5").setSource(XContentType.JSON, "id", "5", "field1", "value").get();
        prepareIndex("test").setId("6").setSource(XContentType.JSON, "id", "6", "field1", "value", "field2", "value").get();
        indicesAdmin().prepareRefresh().get();

        logger.info("percolating empty doc");
        assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", "test", "1", null, null, null)), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        });

        logger.info("percolating doc with 1 field");
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", "test", "5", null, null, null)).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 2);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            }
        );

        logger.info("percolating doc with 2 fields");
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", "test", "6", null, null, null)).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 3);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
    }

    public void testPercolatorQueryExistingDocumentSourceDisabled() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("_source", "enabled=false", "field1", "type=keyword", "query", "type=percolator")
        );

        prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject()).get();

        prepareIndex("test").setId("2").setSource("{}", XContentType.JSON).get();
        indicesAdmin().prepareRefresh().get();

        logger.info("percolating empty doc with source disabled");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            prepareSearch().setQuery(new PercolateQueryBuilder("query", "test", "1", null, null, null)).get();
        });
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testPercolatorSpecificQueries() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=text", "field2", "type=text", "query", "type=percolator")
        );

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "1")
                    .field("query", multiMatchQuery("quick brown fox", "field1", "field2").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "2")
                    .field(
                        "query",
                        spanNearQuery(spanTermQuery("field1", "quick"), 0).addClause(spanTermQuery("field1", "brown"))
                            .addClause(spanTermQuery("field1", "fox"))
                            .inOrder(true)
                    )
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "3")
                    .field(
                        "query",
                        spanNotQuery(
                            spanNearQuery(spanTermQuery("field1", "quick"), 0).addClause(spanTermQuery("field1", "brown"))
                                .addClause(spanTermQuery("field1", "fox"))
                                .inOrder(true),
                            spanNearQuery(spanTermQuery("field1", "the"), 0).addClause(spanTermQuery("field1", "lazy"))
                                .addClause(spanTermQuery("field1", "dog"))
                                .inOrder(true)
                        ).dist(2)
                    )
                    .endObject()
            )
            .get();

        // doesn't match
        prepareIndex("test").setId("4")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "4")
                    .field(
                        "query",
                        spanNotQuery(
                            spanNearQuery(spanTermQuery("field1", "quick"), 0).addClause(spanTermQuery("field1", "brown"))
                                .addClause(spanTermQuery("field1", "fox"))
                                .inOrder(true),
                            spanNearQuery(spanTermQuery("field1", "the"), 0).addClause(spanTermQuery("field1", "lazy"))
                                .addClause(spanTermQuery("field1", "dog"))
                                .inOrder(true)
                        ).dist(3)
                    )
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("field1", "the quick brown fox jumps over the lazy dog")
                .field("field2", "the quick brown fox falls down into the well")
                .endObject()
        );
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 3);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getScore(), equalTo(Float.NaN));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(1).getScore(), equalTo(Float.NaN));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(2).getScore(), equalTo(Float.NaN));
            }
        );
    }

    public void testPercolatorQueryWithHighlighting() throws Exception {
        StringBuilder fieldMapping = new StringBuilder("type=text").append(",store=").append(randomBoolean());
        if (randomBoolean()) {
            fieldMapping.append(",term_vector=with_positions_offsets");
        } else if (randomBoolean()) {
            fieldMapping.append(",index_options=offsets");
        }
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", fieldMapping.toString(), "query", "type=percolator")
        );
        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("id", "1").field("query", matchQuery("field1", "brown fox")).endObject())
            .get();
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("id", "2").field("query", matchQuery("field1", "lazy dog")).endObject())
            .get();
        prepareIndex("test").setId("3")
            .setSource(jsonBuilder().startObject().field("id", "3").field("query", termQuery("field1", "jumps")).endObject())
            .get();
        prepareIndex("test").setId("4")
            .setSource(jsonBuilder().startObject().field("id", "4").field("query", termQuery("field1", "dog")).endObject())
            .get();
        prepareIndex("test").setId("5")
            .setSource(jsonBuilder().startObject().field("id", "5").field("query", termQuery("field1", "fox")).endObject())
            .get();
        indicesAdmin().prepareRefresh().get();

        BytesReference document = BytesReference.bytes(
            jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()
        );
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("query", document, XContentType.JSON))
                .highlighter(new HighlightBuilder().field("field1"))
                .addSort("id", SortOrder.ASC),
            searchResponse -> {
                assertHitCount(searchResponse, 5);

                assertThat(
                    searchResponse.getHits().getAt(0).getHighlightFields().get("field1").fragments()[0].string(),
                    equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog")
                );
                assertThat(
                    searchResponse.getHits().getAt(1).getHighlightFields().get("field1").fragments()[0].string(),
                    equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(2).getHighlightFields().get("field1").fragments()[0].string(),
                    equalTo("The quick brown fox <em>jumps</em> over the lazy dog")
                );
                assertThat(
                    searchResponse.getHits().getAt(3).getHighlightFields().get("field1").fragments()[0].string(),
                    equalTo("The quick brown fox jumps over the lazy <em>dog</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getHighlightFields().get("field1").fragments()[0].string(),
                    equalTo("The quick brown <em>fox</em> jumps over the lazy dog")
                );
            }
        );

        BytesReference document1 = BytesReference.bytes(
            jsonBuilder().startObject().field("field1", "The quick brown fox jumps").endObject()
        );
        BytesReference document2 = BytesReference.bytes(jsonBuilder().startObject().field("field1", "over the lazy dog").endObject());
        assertResponse(
            prepareSearch().setQuery(
                boolQuery().should(new PercolateQueryBuilder("query", document1, XContentType.JSON).setName("query1"))
                    .should(new PercolateQueryBuilder("query", document2, XContentType.JSON).setName("query2"))
            ).highlighter(new HighlightBuilder().field("field1")).addSort("id", SortOrder.ASC),
            searchResponse -> {
                logger.info("searchResponse={}", searchResponse);
                assertHitCount(searchResponse, 5);

                assertThat(
                    searchResponse.getHits().getAt(0).getHighlightFields().get("query1_field1").fragments()[0].string(),
                    equalTo("The quick <em>brown</em> <em>fox</em> jumps")
                );
                assertThat(
                    searchResponse.getHits().getAt(1).getHighlightFields().get("query2_field1").fragments()[0].string(),
                    equalTo("over the <em>lazy</em> <em>dog</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(2).getHighlightFields().get("query1_field1").fragments()[0].string(),
                    equalTo("The quick brown fox <em>jumps</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(3).getHighlightFields().get("query2_field1").fragments()[0].string(),
                    equalTo("over the lazy <em>dog</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getHighlightFields().get("query1_field1").fragments()[0].string(),
                    equalTo("The quick brown <em>fox</em> jumps")
                );
            }
        );

        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder(
                    "query",
                    Arrays.asList(
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "dog").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "fox").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "jumps").endObject()),
                        BytesReference.bytes(jsonBuilder().startObject().field("field1", "brown fox").endObject())
                    ),
                    XContentType.JSON
                )
            ).highlighter(new HighlightBuilder().field("field1")).addSort("id", SortOrder.ASC),
            searchResponse -> {
                assertHitCount(searchResponse, 5);
                assertThat(
                    searchResponse.getHits().getAt(0).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Arrays.asList(1, 3))
                );
                assertThat(
                    searchResponse.getHits().getAt(0).getHighlightFields().get("1_field1").fragments()[0].string(),
                    equalTo("<em>fox</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(0).getHighlightFields().get("3_field1").fragments()[0].string(),
                    equalTo("<em>brown</em> <em>fox</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(1).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Collections.singletonList(0))
                );
                assertThat(
                    searchResponse.getHits().getAt(1).getHighlightFields().get("0_field1").fragments()[0].string(),
                    equalTo("<em>dog</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(2).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Collections.singletonList(2))
                );
                assertThat(
                    searchResponse.getHits().getAt(2).getHighlightFields().get("2_field1").fragments()[0].string(),
                    equalTo("<em>jumps</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(3).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Collections.singletonList(0))
                );
                assertThat(
                    searchResponse.getHits().getAt(3).getHighlightFields().get("0_field1").fragments()[0].string(),
                    equalTo("<em>dog</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Arrays.asList(1, 3))
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getHighlightFields().get("1_field1").fragments()[0].string(),
                    equalTo("<em>fox</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getHighlightFields().get("3_field1").fragments()[0].string(),
                    equalTo("brown <em>fox</em>")
                );
            }
        );

        assertResponse(
            prepareSearch().setQuery(
                boolQuery().should(
                    new PercolateQueryBuilder(
                        "query",
                        Arrays.asList(
                            BytesReference.bytes(jsonBuilder().startObject().field("field1", "dog").endObject()),
                            BytesReference.bytes(jsonBuilder().startObject().field("field1", "fox").endObject())
                        ),
                        XContentType.JSON
                    ).setName("query1")
                )
                    .should(
                        new PercolateQueryBuilder(
                            "query",
                            Arrays.asList(
                                BytesReference.bytes(jsonBuilder().startObject().field("field1", "jumps").endObject()),
                                BytesReference.bytes(jsonBuilder().startObject().field("field1", "brown fox").endObject())
                            ),
                            XContentType.JSON
                        ).setName("query2")
                    )
            ).highlighter(new HighlightBuilder().field("field1")).addSort("id", SortOrder.ASC),
            searchResponse -> {
                logger.info("searchResponse={}", searchResponse);
                assertHitCount(searchResponse, 5);
                assertThat(
                    searchResponse.getHits().getAt(0).getFields().get("_percolator_document_slot_query1").getValues(),
                    equalTo(Collections.singletonList(1))
                );
                assertThat(
                    searchResponse.getHits().getAt(0).getFields().get("_percolator_document_slot_query2").getValues(),
                    equalTo(Collections.singletonList(1))
                );
                assertThat(
                    searchResponse.getHits().getAt(0).getHighlightFields().get("query1_1_field1").fragments()[0].string(),
                    equalTo("<em>fox</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(0).getHighlightFields().get("query2_1_field1").fragments()[0].string(),
                    equalTo("<em>brown</em> <em>fox</em>")
                );

                assertThat(
                    searchResponse.getHits().getAt(1).getFields().get("_percolator_document_slot_query1").getValues(),
                    equalTo(Collections.singletonList(0))
                );
                assertThat(
                    searchResponse.getHits().getAt(1).getHighlightFields().get("query1_0_field1").fragments()[0].string(),
                    equalTo("<em>dog</em>")
                );

                assertThat(
                    searchResponse.getHits().getAt(2).getFields().get("_percolator_document_slot_query2").getValues(),
                    equalTo(Collections.singletonList(0))
                );
                assertThat(
                    searchResponse.getHits().getAt(2).getHighlightFields().get("query2_0_field1").fragments()[0].string(),
                    equalTo("<em>jumps</em>")
                );

                assertThat(
                    searchResponse.getHits().getAt(3).getFields().get("_percolator_document_slot_query1").getValues(),
                    equalTo(Collections.singletonList(0))
                );
                assertThat(
                    searchResponse.getHits().getAt(3).getHighlightFields().get("query1_0_field1").fragments()[0].string(),
                    equalTo("<em>dog</em>")
                );

                assertThat(
                    searchResponse.getHits().getAt(4).getFields().get("_percolator_document_slot_query1").getValues(),
                    equalTo(Collections.singletonList(1))
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getFields().get("_percolator_document_slot_query2").getValues(),
                    equalTo(Collections.singletonList(1))
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getHighlightFields().get("query1_1_field1").fragments()[0].string(),
                    equalTo("<em>fox</em>")
                );
                assertThat(
                    searchResponse.getHits().getAt(4).getHighlightFields().get("query2_1_field1").fragments()[0].string(),
                    equalTo("brown <em>fox</em>")
                );
            }
        );
    }

    public void testTakePositionOffsetGapIntoAccount() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test").setMapping("field", "type=text,position_increment_gap=5", "query", "type=percolator")
        );
        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("query", new MatchPhraseQueryBuilder("field", "brown fox").slop(4)).endObject())
            .get();
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("query", new MatchPhraseQueryBuilder("field", "brown fox").slop(5)).endObject())
            .get();
        indicesAdmin().prepareRefresh().get();

        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder("query", new BytesArray("{\"field\" : [\"brown\", \"fox\"]}"), XContentType.JSON)
            ),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            }
        );
    }

    public void testManyPercolatorFields() throws Exception {
        String queryFieldName = randomAlphaOfLength(8);
        assertAcked(indicesAdmin().prepareCreate("test1").setMapping(queryFieldName, "type=percolator", "field", "type=keyword"));
        assertAcked(
            indicesAdmin().prepareCreate("test2")
                .setMapping(queryFieldName, "type=percolator", "second_query_field", "type=percolator", "field", "type=keyword")
        );
        assertAcked(
            indicesAdmin().prepareCreate("test3")
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
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
                        .endObject()
                        .endObject()
                )
        );
    }

    public void testWithMultiplePercolatorFields() throws Exception {
        String queryFieldName = randomAlphaOfLength(8);
        assertAcked(indicesAdmin().prepareCreate("test1").setMapping(queryFieldName, "type=percolator", "field", "type=keyword"));
        assertAcked(
            indicesAdmin().prepareCreate("test2")
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
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
                        .endObject()
                        .endObject()
                )
        );

        // Acceptable:
        prepareIndex("test1").setId("1")
            .setSource(jsonBuilder().startObject().field(queryFieldName, matchQuery("field", "value")).endObject())
            .get();
        prepareIndex("test2").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startObject("object_field")
                    .field(queryFieldName, matchQuery("field", "value"))
                    .endObject()
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject().field("field", "value").endObject());
        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder(queryFieldName, source, XContentType.JSON)).setIndices("test1"),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getIndex(), equalTo("test1"));
            }
        );

        assertResponse(
            prepareSearch().setQuery(new PercolateQueryBuilder("object_field." + queryFieldName, source, XContentType.JSON))
                .setIndices("test2"),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getIndex(), equalTo("test2"));
            }
        );

        // Unacceptable:
        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> {
            prepareIndex("test2").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .startArray("object_field")
                        .startObject()
                        .field(queryFieldName, matchQuery("field", "value"))
                        .endObject()
                        .startObject()
                        .field(queryFieldName, matchQuery("field", "value"))
                        .endObject()
                        .endArray()
                        .endObject()
                )
                .get();
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("a document can only contain one percolator query"));
    }

    public void testPercolateQueryWithNestedDocuments() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject()
            .startObject("properties")
            .startObject("query")
            .field("type", "percolator")
            .endObject()
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("companyname")
            .field("type", "text")
            .endObject()
            .startObject("employee")
            .field("type", "nested")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(indicesAdmin().prepareCreate("test").setMapping(mapping));
        prepareIndex("test").setId("q1")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "q1")
                    .field(
                        "query",
                        QueryBuilders.nestedQuery(
                            "employee",
                            QueryBuilders.matchQuery("employee.name", "virginia potts").operator(Operator.AND),
                            ScoreMode.Avg
                        )
                    )
                    .endObject()
            )
            .get();
        // this query should never match as it doesn't use nested query:
        prepareIndex("test").setId("q2")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "q2")
                    .field("query", QueryBuilders.matchQuery("employee.name", "virginia"))
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        prepareIndex("test").setId("q3")
            .setSource(jsonBuilder().startObject().field("id", "q3").field("query", QueryBuilders.matchAllQuery()).endObject())
            .get();
        indicesAdmin().prepareRefresh().get();

        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder(
                    "query",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("companyname", "stark")
                            .startArray("employee")
                            .startObject()
                            .field("name", "virginia potts")
                            .endObject()
                            .startObject()
                            .field("name", "tony stark")
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    XContentType.JSON
                )
            ).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 2);
                assertThat(response.getHits().getAt(0).getId(), equalTo("q1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("q3"));
            }
        );

        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder(
                    "query",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("companyname", "notstark")
                            .startArray("employee")
                            .startObject()
                            .field("name", "virginia stark")
                            .endObject()
                            .startObject()
                            .field("name", "tony stark")
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    XContentType.JSON
                )
            ).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("q3"));
            }
        );

        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder(
                    "query",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("companyname", "notstark").endObject()),
                    XContentType.JSON
                )
            ).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("q3"));
            }
        );

        assertResponse(
            prepareSearch().setQuery(
                new PercolateQueryBuilder(
                    "query",
                    Arrays.asList(
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("companyname", "stark")
                                .startArray("employee")
                                .startObject()
                                .field("name", "virginia potts")
                                .endObject()
                                .startObject()
                                .field("name", "tony stark")
                                .endObject()
                                .endArray()
                                .endObject()
                        ),
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("companyname", "stark")
                                .startArray("employee")
                                .startObject()
                                .field("name", "peter parker")
                                .endObject()
                                .startObject()
                                .field("name", "virginia potts")
                                .endObject()
                                .endArray()
                                .endObject()
                        ),
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("companyname", "stark")
                                .startArray("employee")
                                .startObject()
                                .field("name", "peter parker")
                                .endObject()
                                .endArray()
                                .endObject()
                        )
                    ),
                    XContentType.JSON
                )
            ).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 2);
                assertThat(response.getHits().getAt(0).getId(), equalTo("q1"));
                assertThat(
                    response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Arrays.asList(0, 1))
                );
                assertThat(response.getHits().getAt(1).getId(), equalTo("q3"));
                assertThat(
                    response.getHits().getAt(1).getFields().get("_percolator_document_slot").getValues(),
                    equalTo(Arrays.asList(0, 1, 2))
                );
            }
        );
    }

    public void testPercolatorQueryViaMultiSearch() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "query", "type=percolator"));

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
            .get();
        prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject()).get();
        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", boolQuery().must(matchQuery("field1", "b")).must(matchQuery("field1", "c")))
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("4").setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject()).get();
        prepareIndex("test").setId("5").setSource(jsonBuilder().startObject().field("field1", "c").endObject()).get();
        indicesAdmin().prepareRefresh().get();

        assertResponse(
            client().prepareMultiSearch()
                .add(
                    prepareSearch("test").setQuery(
                        new PercolateQueryBuilder(
                            "query",
                            BytesReference.bytes(jsonBuilder().startObject().field("field1", "b").endObject()),
                            XContentType.JSON
                        )
                    )
                )
                .add(
                    prepareSearch("test").setQuery(
                        new PercolateQueryBuilder(
                            "query",
                            BytesReference.bytes(yamlBuilder().startObject().field("field1", "c").endObject()),
                            XContentType.YAML
                        )
                    )
                )
                .add(
                    prepareSearch("test").setQuery(
                        new PercolateQueryBuilder(
                            "query",
                            BytesReference.bytes(jsonBuilder().startObject().field("field1", "b c").endObject()),
                            XContentType.JSON
                        )
                    )
                )
                .add(
                    prepareSearch("test").setQuery(
                        new PercolateQueryBuilder(
                            "query",
                            BytesReference.bytes(jsonBuilder().startObject().field("field1", "d").endObject()),
                            XContentType.JSON
                        )
                    )
                )
                .add(prepareSearch("test").setQuery(new PercolateQueryBuilder("query", "test", "5", null, null, null)))
                .add(
                    prepareSearch("test") // non existing doc, so error element
                        .setQuery(new PercolateQueryBuilder("query", "test", "6", null, null, null))
                ),
            response -> {
                Item item = response.getResponses()[0];
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
        );
    }

    public void testDisallowExpensiveQueries() throws IOException {
        try {
            assertAcked(
                indicesAdmin().prepareCreate("test").setMapping("id", "type=keyword", "field1", "type=keyword", "query", "type=percolator")
            );

            prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject().field("id", "1").field("query", matchQuery("field1", "value")).endObject())
                .get();
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = false => success
            BytesReference source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject());
            assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
            });

            // Set search.allow_expensive_queries to "false" => assert failure
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", false));

            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)).get()
            );
            assertEquals(
                "[percolate] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getCause().getMessage()
            );

            // Set search.allow_expensive_queries setting to "true" ==> success
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", true));

            assertResponse(prepareSearch().setQuery(new PercolateQueryBuilder("query", source, XContentType.JSON)), response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getFields().get("_percolator_document_slot").getValue(), equalTo(0));
            });
        } finally {
            updateClusterSettings(Settings.builder().putNull("search.allow_expensive_queries"));
        }
    }

    public void testWrappedWithConstantScore() throws Exception {

        assertAcked(indicesAdmin().prepareCreate("test").setMapping("d", "type=date", "q", "type=percolator"));

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("q", boolQuery().must(rangeQuery("d").gt("now"))).endObject())
            .get();

        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("q", boolQuery().must(rangeQuery("d").lt("now"))).endObject())
            .get();

        indicesAdmin().prepareRefresh().get();

        assertHitCount(
            prepareSearch("test").setQuery(
                new PercolateQueryBuilder(
                    "q",
                    BytesReference.bytes(jsonBuilder().startObject().field("d", "2020-02-01T15:00:00.000+11:00").endObject()),
                    XContentType.JSON
                )
            ),
            1
        );

        assertHitCount(
            prepareSearch("test").setQuery(
                new PercolateQueryBuilder(
                    "q",
                    BytesReference.bytes(jsonBuilder().startObject().field("d", "2020-02-01T15:00:00.000+11:00").endObject()),
                    XContentType.JSON
                )
            ).addSort("_doc", SortOrder.ASC),
            1
        );

        assertHitCount(
            prepareSearch("test").setQuery(
                constantScoreQuery(
                    new PercolateQueryBuilder(
                        "q",
                        BytesReference.bytes(jsonBuilder().startObject().field("d", "2020-02-01T15:00:00.000+11:00").endObject()),
                        XContentType.JSON
                    )
                )
            ),
            1
        );
    }

    public void testWithWildcardFieldNames() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "text_1",
                    "type=text",
                    "q_simple",
                    "type=percolator",
                    "q_string",
                    "type=percolator",
                    "q_match",
                    "type=percolator",
                    "q_combo",
                    "type=percolator"
                )
        );

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("q_simple", simpleQueryStringQuery("yada").fields(Map.of("text*", 1f)))
                    .field("q_string", queryStringQuery("yada").fields(Map.of("text*", 1f)))
                    .field("q_match", multiMatchQuery("yada", "text*"))
                    .field("q_combo", combinedFieldsQuery("yada", "text*"))
                    .endObject()
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertHitCount(
            prepareSearch("test").setQuery(
                new PercolateQueryBuilder(
                    "q_simple",
                    BytesReference.bytes(jsonBuilder().startObject().field("text_1", "yada").endObject()),
                    XContentType.JSON
                )
            ),
            1
        );

        assertHitCount(
            prepareSearch("test").setQuery(
                new PercolateQueryBuilder(
                    "q_string",
                    BytesReference.bytes(jsonBuilder().startObject().field("text_1", "yada").endObject()),
                    XContentType.JSON
                )
            ),
            1
        );

        assertHitCount(
            prepareSearch("test").setQuery(
                new PercolateQueryBuilder(
                    "q_match",
                    BytesReference.bytes(jsonBuilder().startObject().field("text_1", "yada").endObject()),
                    XContentType.JSON
                )
            ),
            1
        );

        assertHitCount(
            prepareSearch("test").setQuery(
                new PercolateQueryBuilder(
                    "q_combo",
                    BytesReference.bytes(jsonBuilder().startObject().field("text_1", "yada").endObject()),
                    XContentType.JSON
                )
            ),
            1
        );
    }

    public void testKnnQueryNotSupportedInPercolator() throws IOException {
        String mappings = org.elasticsearch.common.Strings.format("""
            {
              "properties": {
                "my_query" : {
                  "type" : "percolator"
                },
                "my_vector" : {
                  "type" : "dense_vector",
                  "dims" : 5,
                  "index" : true,
                  "similarity" : "l2_norm"
                }

              }
            }
            """);
        indicesAdmin().prepareCreate("index1").setMapping(mappings).get();
        ensureGreen();
        QueryBuilder knnVectorQueryBuilder = new KnnVectorQueryBuilder("my_vector", new float[] { 1, 1, 1, 1, 1 }, 10, 10, null, null);

        IndexRequestBuilder indexRequestBuilder = prepareIndex("index1").setId("knn_query1")
            .setSource(jsonBuilder().startObject().field("my_query", knnVectorQueryBuilder).endObject());

        DocumentParsingException exception = expectThrows(DocumentParsingException.class, () -> indexRequestBuilder.get());
        assertThat(exception.getMessage(), containsString("the [knn] query is unsupported inside a percolator"));
    }

}
