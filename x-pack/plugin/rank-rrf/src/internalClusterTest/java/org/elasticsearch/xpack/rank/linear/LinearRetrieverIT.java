/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.search.vectors.TestQueryVectorBuilderPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.rank.rrf.RRFRankPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class LinearRetrieverIT extends ESIntegTestCase {

    protected static String INDEX = "test_index";
    protected static final String DOC_FIELD = "doc";
    protected static final String TEXT_FIELD = "text";
    protected static final String VECTOR_FIELD = "vector";
    protected static final String TOPIC_FIELD = "topic";

    private BytesReference pitId;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RRFRankPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        setupIndex();
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(INDEX).keepAlive(TimeValue.timeValueMinutes(2));
        OpenPointInTimeResponse openResp = client().execute(TransportOpenPointInTimeAction.TYPE, openRequest).actionGet();
        pitId = openResp.getPointInTimeId();
    }

    @After
    public void cleanup() {
        if (pitId != null) {
            try {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet(30, TimeUnit.SECONDS);
                logger.info("Closed PIT successfully");
                pitId = null;
                Thread.sleep(100);
            } catch (Exception e) {
                logger.error("Error closing point in time", e);
            }
        }
    }

    protected SearchRequestBuilder prepareSearchWithPIT(SearchSourceBuilder source) {
        return client().prepareSearch()
            .setSource(source.pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(1))))
            .setPreference(null);
    }

    protected void setupIndex() {
        String mapping = """
            {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "dims": 1,
                  "element_type": "float",
                  "similarity": "l2_norm",
                  "index": true,
                  "index_options": {
                    "type": "flat"
                  }
                },
                "text": {
                  "type": "text"
                },
                "doc": {
                  "type": "keyword"
                },
                "topic": {
                  "type": "keyword"
                },
                "views": {
                    "type": "nested",
                    "properties": {
                        "last30d": {
                            "type": "integer"
                        },
                        "all": {
                            "type": "integer"
                        }
                    }
                }
              }
            }
            """;
        createIndex(INDEX, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5)).build());
        admin().indices().preparePutMapping(INDEX).setSource(mapping, XContentType.JSON).get();
        indexDoc(INDEX, "doc_1", DOC_FIELD, "doc_1", TOPIC_FIELD, "technology", TEXT_FIELD, "term");
        indexDoc(
            INDEX,
            "doc_2",
            DOC_FIELD,
            "doc_2",
            TOPIC_FIELD,
            "astronomy",
            TEXT_FIELD,
            "search term term",
            VECTOR_FIELD,
            new float[] { 2.0f }
        );
        indexDoc(INDEX, "doc_3", DOC_FIELD, "doc_3", TOPIC_FIELD, "technology", VECTOR_FIELD, new float[] { 3.0f });
        indexDoc(INDEX, "doc_4", DOC_FIELD, "doc_4", TOPIC_FIELD, "technology", TEXT_FIELD, "term term term term");
        indexDoc(INDEX, "doc_5", DOC_FIELD, "doc_5", TOPIC_FIELD, "science", TEXT_FIELD, "irrelevant stuff");
        indexDoc(
            INDEX,
            "doc_6",
            DOC_FIELD,
            "doc_6",
            TEXT_FIELD,
            "search term term term term term term",
            VECTOR_FIELD,
            new float[] { 6.0f }
        );
        indexDoc(
            INDEX,
            "doc_7",
            DOC_FIELD,
            "doc_7",
            TOPIC_FIELD,
            "biology",
            TEXT_FIELD,
            "term term term term term term term",
            VECTOR_FIELD,
            new float[] { 7.0f }
        );
        refresh(INDEX);
        ensureGreen(INDEX);
    }

    public void testLinearRetrieverWithAggs() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this one retrieves docs 1, 2, 4, 6, and 7
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        // this one retrieves docs 2 and 6 due to prefilter
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        // this one retrieves docs 2, 3, 6, and 7
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);

        // all requests would have an equal weight and use the identity normalizer
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize
            )
        );
        source.size(1);
        source.aggregation(AggregationBuilders.terms("topic_agg").field(TOPIC_FIELD));
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(1));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));

            assertNotNull(resp.getAggregations());
            assertNotNull(resp.getAggregations().get("topic_agg"));
            Terms terms = resp.getAggregations().get("topic_agg");

            assertThat(terms.getBucketByKey("technology").getDocCount(), equalTo(3L));
            assertThat(terms.getBucketByKey("astronomy").getDocCount(), equalTo(1L));
            assertThat(terms.getBucketByKey("biology").getDocCount(), equalTo(1L));
        });
    }

    public void testLinearWithCollapse() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this one retrieves docs 1, 2, 4, 6, and 7
        // with scores 10, 9, 8, 7, 6
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        // this one retrieves docs 2 and 6 due to prefilter
        // with scores 20, 5
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        // this one retrieves docs 2, 3, 6, and 7
        // with scores 1, 0.5, 0.05882353, 0.03846154
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);
        // final ranking with no-normalizer would be: doc 2, 6, 1, 4, 7, 3
        // doc 1: 10
        // doc 2: 9 + 20 + 1 = 30
        // doc 3: 0.5
        // doc 4: 8
        // doc 6: 7 + 5 + 0.05882353 = 12.05882353
        // doc 7: 6 + 0.03846154 = 6.03846154
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize
            )
        );
        source.collapse(
            new CollapseBuilder(TOPIC_FIELD).setInnerHits(
                new InnerHitBuilder("a").addSort(new FieldSortBuilder(DOC_FIELD).order(SortOrder.DESC)).setSize(10)
            )
        );
        source.fetchField(TOPIC_FIELD);
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(4));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
            assertThat(resp.getHits().getAt(0).getScore(), equalTo(30f));
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_6"));
            assertThat((double) resp.getHits().getAt(1).getScore(), closeTo(12.0588f, 0.0001f));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(2).getInnerHits().get("a").getAt(0).getId(), equalTo("doc_4"));
            assertThat(resp.getHits().getAt(2).getInnerHits().get("a").getAt(1).getId(), equalTo("doc_3"));
            assertThat(resp.getHits().getAt(2).getInnerHits().get("a").getAt(2).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_7"));
            assertThat((double) resp.getHits().getAt(3).getScore(), closeTo(6.0384f, 0.0001f));
        });
    }

    public void testLinearRetrieverWithCollapseAndAggs() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this one retrieves docs 1, 2, 4, 6, and 7
        // with scores 10, 9, 8, 7, 6
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        // this one retrieves docs 2 and 6 due to prefilter
        // with scores 20, 5
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        // this one retrieves docs 2, 3, 6, and 7
        // with scores 1, 0.5, 0.05882353, 0.03846154
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);
        // final ranking with no-normalizer would be: doc 2, 6, 1, 4, 7, 3
        // doc 1: 10
        // doc 2: 9 + 20 + 1 = 30
        // doc 3: 0.5
        // doc 4: 8
        // doc 6: 7 + 5 + 0.05882353 = 12.05882353
        // doc 7: 6 + 0.03846154 = 6.03846154
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize
            )
        );
        source.collapse(
            new CollapseBuilder(TOPIC_FIELD).setInnerHits(
                new InnerHitBuilder("a").addSort(new FieldSortBuilder(DOC_FIELD).order(SortOrder.DESC)).setSize(10)
            )
        );
        source.fetchField(TOPIC_FIELD);
        source.aggregation(AggregationBuilders.terms("topic_agg").field(TOPIC_FIELD));
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(4));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_6"));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(2).getInnerHits().get("a").getAt(0).getId(), equalTo("doc_4"));
            assertThat(resp.getHits().getAt(2).getInnerHits().get("a").getAt(1).getId(), equalTo("doc_3"));
            assertThat(resp.getHits().getAt(2).getInnerHits().get("a").getAt(2).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_7"));

            assertNotNull(resp.getAggregations());
            assertNotNull(resp.getAggregations().get("topic_agg"));
            Terms terms = resp.getAggregations().get("topic_agg");

            assertThat(terms.getBucketByKey("technology").getDocCount(), equalTo(3L));
            assertThat(terms.getBucketByKey("astronomy").getDocCount(), equalTo(1L));
            assertThat(terms.getBucketByKey("biology").getDocCount(), equalTo(1L));
        });
    }

    public void testMultipleLinearRetrievers() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this one retrieves docs 1, 2, 4, 6, and 7
        // with scores 10, 9, 8, 7, 6
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        // this one retrieves docs 2 and 6 due to prefilter
        // with scores 20, 5
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(
                        // this one returns docs doc 2, 1, 6, 4, 7
                        // with scores 38, 20, 19, 16, 12
                        new LinearRetrieverBuilder(
                            Arrays.asList(
                                new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                                new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                            ),
                            rankWindowSize,
                            new float[] { 2.0f, 1.0f },
                            new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE },
                            0.0f
                        ),
                        null
                    ),
                    // this one bring just doc 7 which should be ranked first eventually with a score of 100
                    new CompoundRetrieverBuilder.RetrieverSource(
                        new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 7.0f }, null, 1, 100, null, null),
                        null
                    )
                ),
                rankWindowSize,
                new float[] { 1.0f, 100.0f },
                new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE },
                0.0f
            )
        );

        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(5L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_7"));
            assertThat(resp.getHits().getAt(0).getScore(), equalTo(112f));
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_2"));
            assertThat(resp.getHits().getAt(1).getScore(), equalTo(38f));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(2).getScore(), equalTo(20f));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_6"));
            assertThat(resp.getHits().getAt(3).getScore(), equalTo(19f));
            assertThat(resp.getHits().getAt(4).getId(), equalTo("doc_4"));
            assertThat(resp.getHits().getAt(4).getScore(), equalTo(16f));
        });
    }

    public void testLinearExplainWithNamedRetrievers() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this one retrieves docs 1, 2, 4, 6, and 7
        // with scores 10, 9, 8, 7, 6
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        standard0.retrieverName("my_custom_retriever");
        // this one retrieves docs 2 and 6 due to prefilter
        // with scores 20, 5
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        // this one retrieves docs 2, 3, 6, and 7
        // with scores 1, 0.5, 0.05882353, 0.03846154
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);
        // final ranking with no-normalizer would be: doc 2, 6, 1, 4, 7, 3
        // doc 1: 10
        // doc 2: 9 + 20 + 1 = 30
        // doc 3: 0.5
        // doc 4: 8
        // doc 6: 7 + 5 + 0.05882353 = 12.05882353
        // doc 7: 6 + 0.03846154 = 6.03846154
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize
            )
        );
        source.explain(true);
        source.size(1);
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(1));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
            assertThat(resp.getHits().getAt(0).getExplanation().isMatch(), equalTo(true));
            assertThat(resp.getHits().getAt(0).getExplanation().getDescription(), containsString("sum of:"));
            assertThat(resp.getHits().getAt(0).getExplanation().getDetails().length, equalTo(2));
            var linearTopLevel = resp.getHits().getAt(0).getExplanation().getDetails()[0];
            assertThat(linearTopLevel.getDetails().length, equalTo(3));
            assertThat(
                linearTopLevel.getDescription(),
                containsString(
                    "weighted linear combination score: [30.0] computed for normalized scores [9.0, 20.0, 1.0] "
                        + "and weights [1.0, 1.0, 1.0] as sum of (weight[i] * score[i]) for each query."
                )
            );

            assertThat(
                linearTopLevel.getDetails()[0].getDescription(),
                containsString(
                    "weighted score: [9.0] in query at index [0] [my_custom_retriever] computed as [1.0 * 9.0] "
                        + "using score normalizer [none] for original matching query with score"
                )
            );
            assertThat(
                linearTopLevel.getDetails()[1].getDescription(),
                containsString(
                    "weighted score: [20.0] in query at index [1] computed as [1.0 * 20.0] using score normalizer [none] "
                        + "for original matching query with score:"
                )
            );
            assertThat(
                linearTopLevel.getDetails()[2].getDescription(),
                containsString(
                    "weighted score: [1.0] in query at index [2] computed as [1.0 * 1.0] using score normalizer [none] "
                        + "for original matching query with score"
                )
            );
        });
    }

    public void testLinearExplainWithAnotherNestedLinear() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this one retrieves docs 1, 2, 4, 6, and 7
        // with scores 10, 9, 8, 7, 6
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        standard0.retrieverName("my_custom_retriever");
        // this one retrieves docs 2 and 6 due to prefilter
        // with scores 20, 5
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        // this one retrieves docs 2, 3, 6, and 7
        // with scores 1, 0.5, 0.05882353, 0.03846154
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);
        // final ranking with no-normalizer would be: doc 2, 6, 1, 4, 7, 3
        // doc 1: 10
        // doc 2: 9 + 20 + 1 = 30
        // doc 3: 0.5
        // doc 4: 8
        // doc 6: 7 + 5 + 0.05882353 = 12.05882353
        // doc 7: 6 + 0.03846154 = 6.03846154
        LinearRetrieverBuilder nestedLinear = new LinearRetrieverBuilder(
            Arrays.asList(
                new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
            ),
            rankWindowSize
        );
        nestedLinear.retrieverName("nested_linear");
        // this one retrieves docs 6 with a score of 100
        StandardRetrieverBuilder standard2 = new StandardRetrieverBuilder(
            QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(20L)
        );
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(nestedLinear, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard2, null)
                ),
                rankWindowSize,
                new float[] { 1.0f, 5f },
                new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE },
                0.0f
            )
        );
        source.explain(true);
        source.size(1);
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(1));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_6"));
            assertThat(resp.getHits().getAt(0).getExplanation().isMatch(), equalTo(true));
            assertThat(resp.getHits().getAt(0).getExplanation().getDescription(), containsString("sum of:"));
            assertThat(resp.getHits().getAt(0).getExplanation().getDetails().length, equalTo(2));
            var linearTopLevel = resp.getHits().getAt(0).getExplanation().getDetails()[0];
            assertThat(linearTopLevel.getDetails().length, equalTo(2));
            assertThat(
                linearTopLevel.getDescription(),
                containsString(
                    "weighted linear combination score: [112.05882] computed for normalized scores [12.058824, 20.0] "
                        + "and weights [1.0, 5.0] as sum of (weight[i] * score[i]) for each query."
                )
            );
            assertThat(linearTopLevel.getDetails()[0].getDescription(), containsString("weighted score: [12.058824]"));
            assertThat(linearTopLevel.getDetails()[0].getDescription(), containsString("nested_linear"));
            assertThat(linearTopLevel.getDetails()[1].getDescription(), containsString("weighted score: [100.0]"));

            var linearNested = linearTopLevel.getDetails()[0];
            assertThat(linearNested.getDetails()[0].getDetails().length, equalTo(3));
            assertThat(linearNested.getDetails()[0].getDetails()[0].getDescription(), containsString("weighted score: [7.0]"));
            assertThat(linearNested.getDetails()[0].getDetails()[1].getDescription(), containsString("weighted score: [5.0]"));
            assertThat(linearNested.getDetails()[0].getDetails()[2].getDescription(), containsString("weighted score: [0.05882353]"));

            var standard0Details = linearTopLevel.getDetails()[1];
            assertThat(standard0Details.getDetails()[0].getDescription(), containsString("ConstantScore"));
        });
    }

    public void testLinearInnerRetrieverAll4xxSearchErrors() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this will throw a 4xx error during evaluation
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.constantScoreQuery(QueryBuilders.rangeQuery(VECTOR_FIELD).gte(10))
        );
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                ),
                rankWindowSize
            )
        );
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        Exception ex = expectThrows(ElasticsearchStatusException.class, req::get);
        assertThat(ex, instanceOf(ElasticsearchStatusException.class));
        assertThat(
            ex.getMessage(),
            containsString(
                "[linear] search failed - retrievers '[standard]' returned errors. All failures are attached as suppressed exceptions."
            )
        );
        assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ex.getSuppressed().length, equalTo(1));
        assertThat(ex.getSuppressed()[0].getCause().getCause(), instanceOf(IllegalArgumentException.class));
    }

    public void testLinearInnerRetrieverMultipleErrorsOne5xx() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this will throw a 4xx error during evaluation
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.constantScoreQuery(QueryBuilders.rangeQuery(VECTOR_FIELD).gte(10))
        );
        // this will throw a 5xx error
        TestRetrieverBuilder testRetrieverBuilder = new TestRetrieverBuilder("val") {
            @Override
            public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                searchSourceBuilder.aggregation(AggregationBuilders.avg("some_invalid_param"));
            }
        };
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(testRetrieverBuilder, null)
                ),
                rankWindowSize
            )
        );
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        Exception ex = expectThrows(ElasticsearchStatusException.class, req::get);
        assertThat(ex, instanceOf(ElasticsearchStatusException.class));
        assertThat(
            ex.getMessage(),
            containsString(
                "[linear] search failed - retrievers '[standard, test]' returned errors. "
                    + "All failures are attached as suppressed exceptions."
            )
        );
        assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(ex.getSuppressed().length, equalTo(2));
        assertThat(ex.getSuppressed()[0].getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(ex.getSuppressed()[1].getCause().getCause(), instanceOf(IllegalStateException.class));
    }

    public void testLinearInnerRetrieverErrorWhenExtractingToSource() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        TestRetrieverBuilder failingRetriever = new TestRetrieverBuilder("some value") {
            @Override
            public QueryBuilder topDocsQuery() {
                return QueryBuilders.matchAllQuery();
            }

            @Override
            public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                throw new UnsupportedOperationException("simulated failure");
            }
        };
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(failingRetriever, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                ),
                rankWindowSize
            )
        );
        source.size(1);
        expectThrows(UnsupportedOperationException.class, () -> client().prepareSearch(INDEX).setSource(source).get());
    }

    public void testLinearInnerRetrieverErrorOnTopDocs() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        TestRetrieverBuilder failingRetriever = new TestRetrieverBuilder("some value") {
            @Override
            public QueryBuilder topDocsQuery() {
                throw new UnsupportedOperationException("simulated failure");
            }

            @Override
            public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            }
        };
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(failingRetriever, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                ),
                rankWindowSize
            )
        );
        source.size(1);
        source.aggregation(AggregationBuilders.terms("topic_agg").field(TOPIC_FIELD));
        expectThrows(UnsupportedOperationException.class, () -> client().prepareSearch(INDEX).setSource(source).get());
    }

    public void testLinearFiltersPropagatedToKnnQueryVectorBuilder() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        // this will retriever all but 7 only due to top-level filter
        StandardRetrieverBuilder standardRetriever = new StandardRetrieverBuilder(QueryBuilders.matchAllQuery());
        // this will too retrieve just doc 7
        KnnRetrieverBuilder knnRetriever = new KnnRetrieverBuilder(
            "vector",
            null,
            new TestQueryVectorBuilderPlugin.TestQueryVectorBuilder(new float[] { 3 }),
            10,
            10,
            null,
            null
        );
        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standardRetriever, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetriever, null)
                ),
                rankWindowSize
            )
        );
        source.retriever().getPreFilterQueryBuilders().add(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(DOC_FIELD, "doc_7")));
        source.size(10);
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(resp.getHits().getHits()[0].getId(), equalTo("doc_7"));
        });
    }

    public void testRewriteOnce() {
        final float[] vector = new float[] { 1 };
        AtomicInteger numAsyncCalls = new AtomicInteger();
        QueryVectorBuilder vectorBuilder = new QueryVectorBuilder() {
            @Override
            public void buildVector(Client client, ActionListener<float[]> listener) {
                numAsyncCalls.incrementAndGet();
                listener.onResponse(vector);
            }

            @Override
            public String getWriteableName() {
                throw new IllegalStateException("Should not be called");
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                throw new IllegalStateException("Should not be called");
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new IllegalStateException("Should not be called");
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new IllegalStateException("Should not be called");
            }
        };
        var knn = new KnnRetrieverBuilder("vector", null, vectorBuilder, 10, 10, null, null);
        var standard = new StandardRetrieverBuilder(new KnnVectorQueryBuilder("vector", vectorBuilder, 10, 10, null));
        var rrf = new LinearRetrieverBuilder(
            List.of(new CompoundRetrieverBuilder.RetrieverSource(knn, null), new CompoundRetrieverBuilder.RetrieverSource(standard, null)),
            10
        );
        assertResponse(
            client().prepareSearch(INDEX).setSource(new SearchSourceBuilder().retriever(rrf)),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), is(4L))
        );
        assertThat(numAsyncCalls.get(), equalTo(2));

        // check that we use the rewritten vector to build the explain query
        assertResponse(
            client().prepareSearch(INDEX).setSource(new SearchSourceBuilder().retriever(rrf).explain(true)),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), is(4L))
        );
        assertThat(numAsyncCalls.get(), equalTo(4));
    }

    public void testLinearWithMinScore() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);

        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                new float[] { 1.0f, 1.0f, 1.0f },
                new ScoreNormalizer[] {
                    IdentityScoreNormalizer.INSTANCE,
                    IdentityScoreNormalizer.INSTANCE,
                    IdentityScoreNormalizer.INSTANCE },
                25.0f
            )
        );

        SearchRequestBuilder req = prepareSearchWithPIT(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNotNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(1));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
            assertThat((double) resp.getHits().getAt(0).getScore(), closeTo(30.0f, 0.001f));
        });

        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                new float[] { 1.0f, 1.0f, 1.0f },
                new ScoreNormalizer[] {
                    IdentityScoreNormalizer.INSTANCE,
                    IdentityScoreNormalizer.INSTANCE,
                    IdentityScoreNormalizer.INSTANCE },
                10.0f
            )
        );
        req = prepareSearchWithPIT(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNotNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(3L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(3));
            assertThat(resp.getHits().getAt(0).getScore(), equalTo(30.0f));
            for (int i = 0; i < resp.getHits().getHits().length; i++) {
                assertThat("Document at position " + i + " has score >= 10.0", resp.getHits().getAt(i).getScore() >= 10.0f, equalTo(true));
            }
        });
    }

    public void testLinearWithMinScoreAndNormalization() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_1")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(9L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_4")).boost(8L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(7L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_7")).boost(6L))
        );
        StandardRetrieverBuilder standard1 = new StandardRetrieverBuilder(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_2")).boost(20L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_3")).boost(10L))
                .should(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(5L))
        );
        standard1.getPreFilterQueryBuilders().add(QueryBuilders.queryStringQuery("search").defaultField(TEXT_FIELD));
        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 2.0f }, null, 10, 100, null, null);

        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                new float[] { 1.0f, 1.0f, 1.0f },
                new ScoreNormalizer[] { MinMaxScoreNormalizer.INSTANCE, MinMaxScoreNormalizer.INSTANCE, MinMaxScoreNormalizer.INSTANCE },
                0.8f
            )
        );

        SearchRequestBuilder req = prepareSearchWithPIT(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNotNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(4L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getHits().length, equalTo(4));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
            assertThat((double) resp.getHits().getAt(0).getScore(), closeTo(1.9f, 0.1f));
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_1"));
            assertThat((double) resp.getHits().getAt(1).getScore(), closeTo(1.0f, 0.1f));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_6"));
            assertThat((double) resp.getHits().getAt(2).getScore(), closeTo(0.95f, 0.1f));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_4"));
            assertThat((double) resp.getHits().getAt(3).getScore(), closeTo(0.8f, 0.1f));
        });

        source.retriever(
            new LinearRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                new float[] { 1.0f, 1.0f, 1.0f },
                new ScoreNormalizer[] { MinMaxScoreNormalizer.INSTANCE, MinMaxScoreNormalizer.INSTANCE, MinMaxScoreNormalizer.INSTANCE },
                0.95f
            )
        );

        req = prepareSearchWithPIT(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNotNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getHits().length, equalTo(3));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
            assertThat((double) resp.getHits().getAt(0).getScore(), closeTo(1.9f, 0.1f));
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_1"));
            assertThat((double) resp.getHits().getAt(1).getScore(), closeTo(1.0f, 0.1f));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_6"));
            assertThat((double) resp.getHits().getAt(2).getScore(), closeTo(0.95f, 0.1f));
        });
    }

    public void testLinearWithMinScoreValidation() {
        final int rankWindowSize = 100;
        SearchSourceBuilder source = new SearchSourceBuilder();
        StandardRetrieverBuilder standard0 = new StandardRetrieverBuilder(QueryBuilders.matchAllQuery());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new LinearRetrieverBuilder(
                Arrays.asList(new CompoundRetrieverBuilder.RetrieverSource(standard0, null)),
                rankWindowSize,
                new float[] { 1.0f },
                new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE },
                -1.0f
            )
        );
        assertThat(e.getMessage(), equalTo("[min_score] must be greater than 0, was: -1.0"));
    }

    public void testLinearRetrieverRankWindowSize() {
        final int rankWindowSize = 3;

        createTestDocuments(10);

        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().pointInTimeBuilder(
                new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(1))
            );

            StandardRetrieverBuilder retriever1 = new StandardRetrieverBuilder(QueryBuilders.matchAllQuery());
            StandardRetrieverBuilder retriever2 = new StandardRetrieverBuilder(QueryBuilders.matchAllQuery());

            LinearRetrieverBuilder linearRetrieverBuilder = new LinearRetrieverBuilder(
                List.of(
                    new CompoundRetrieverBuilder.RetrieverSource(retriever1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(retriever2, null)
                ),
                rankWindowSize,
                new float[] { 1.0f, 1.0f },
                new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE },
                0.0f
            );

            searchSourceBuilder.retriever(linearRetrieverBuilder);

            ElasticsearchAssertions.assertResponse(prepareSearchWithPIT(searchSourceBuilder), response -> {
                assertNotNull("PIT ID should be present", response.pointInTimeId());
                assertNotNull("Hit count should be present", response.getHits().getTotalHits());

                assertThat(
                    "Number of hits should be limited by rank window size",
                    response.getHits().getHits().length,
                    equalTo(rankWindowSize)
                );
            });

            Thread.sleep(100);
        } catch (Exception e) {
            fail("Failed to execute search: " + e.getMessage());
        }
    }

    private void createTestDocuments(int count) {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            builders.add(client().prepareIndex(INDEX).setSource(DOC_FIELD, "doc" + i, TEXT_FIELD, "text" + i));
        }
        indexRandom(true, builders);
        ensureSearchable(INDEX);
    }
}
