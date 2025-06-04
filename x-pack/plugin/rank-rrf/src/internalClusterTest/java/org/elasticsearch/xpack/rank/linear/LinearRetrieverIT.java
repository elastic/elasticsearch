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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
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
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RRFRankPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        setupIndex();
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
            assertThat(resp.getHits().getAt(2).getScore(), equalTo(10f));
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
                            new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE }
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
                new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE }
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
            var rrfDetails = resp.getHits().getAt(0).getExplanation().getDetails()[0];
            assertThat(rrfDetails.getDetails().length, equalTo(3));
            assertThat(
                rrfDetails.getDescription(),
                equalTo(
                    "weighted linear combination score: [30.0] computed for normalized scores [9.0, 20.0, 1.0] "
                        + "and weights [1.0, 1.0, 1.0] as sum of (weight[i] * score[i]) for each query."
                )
            );

            assertThat(
                rrfDetails.getDetails()[0].getDescription(),
                containsString(
                    "weighted score: [9.0] in query at index [0] [my_custom_retriever] computed as [1.0 * 9.0] "
                        + "using score normalizer [none] for original matching query with score"
                )
            );
            assertThat(
                rrfDetails.getDetails()[1].getDescription(),
                containsString(
                    "weighted score: [20.0] in query at index [1] computed as [1.0 * 20.0] using score normalizer [none] "
                        + "for original matching query with score:"
                )
            );
            assertThat(
                rrfDetails.getDetails()[2].getDescription(),
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
                new float[] { 1, 5f },
                new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE }
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

    public void testLinearRetrieverWithMinScoreValidation() {
        StandardRetrieverBuilder retriever1 = new StandardRetrieverBuilder(new MatchAllQueryBuilder());
        float[] weights = new float[] { 1.0f };
        ScoreNormalizer[] normalizers = LinearRetrieverBuilder.getDefaultNormalizers(1);
        LinearRetrieverBuilder builder = new LinearRetrieverBuilder(
            List.of(new CompoundRetrieverBuilder.RetrieverSource(retriever1, null)),
            10,
            weights,
            normalizers
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.minScore(-0.1f));
        assertThat(e.getMessage(), equalTo("[min_score] must be greater than or equal to 0, was: -0.1"));

        builder.minScore(0.1f);
        assertThat(builder.minScore(), equalTo(0.1f));
    }

    // public void testLinearRetrieverWithMinScoreScenarios() {
    // final int rankWindowSize = 10;

    // // Setup test data
    // indexDoc(INDEX, "doc_1", TEXT_FIELD, "term1", "views.last30d", 10, "views.all", 100);
    // indexDoc(INDEX, "doc_2", TEXT_FIELD, "term1 term2", "views.last30d", 20, "views.all", 200);
    // indexDoc(INDEX, "doc_3", TEXT_FIELD, "term1 term2 term3", "views.last30d", 30, "views.all", 300);
    // indexDoc(INDEX, "doc_4", TEXT_FIELD, "term4", "views.last30d", 40, "views.all", 400);
    // refresh(INDEX);

    // // Create retrievers with different scoring
    // StandardRetrieverBuilder retrieverA = new StandardRetrieverBuilder(QueryBuilders.termQuery(TEXT_FIELD, "term1").boost(10.0f));
    // StandardRetrieverBuilder retrieverB = new StandardRetrieverBuilder(QueryBuilders.termQuery(TEXT_FIELD, "term2").boost(1.0f));

    // float[] weights = new float[] { 1.0f, 1.0f };
    // ScoreNormalizer[] identityNormalizers = LinearRetrieverBuilder.getDefaultNormalizers(2);

    // // Scenario 1: No min_score - all docs returned
    // LinearRetrieverBuilder builderNoMinScore = new LinearRetrieverBuilder(
    // List.of(
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverA, null),
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverB, null)
    // ),
    // rankWindowSize,
    // weights,
    // identityNormalizers
    // );

    // SearchSourceBuilder sourceNoMinScore = new SearchSourceBuilder().retriever(builderNoMinScore).size(rankWindowSize);

    // ElasticsearchAssertions.assertResponse(client().prepareSearch(INDEX).setSource(sourceNoMinScore), resp -> {
    // assertThat(resp.getHits().getTotalHits().value(), equalTo(3L)); // doc_1, doc_2, doc_3 match
    // assertThat(resp.getHits().getHits()[0].getId(), equalTo("doc_3")); // term1(10) + term2(1) = 11
    // assertThat(resp.getHits().getHits()[1].getId(), equalTo("doc_2")); // term1(10) + term2(1) = 11
    // assertThat(resp.getHits().getHits()[2].getId(), equalTo("doc_1")); // term1(10) = 10
    // });

    // // Scenario 2: minScore = 0.0f - all matching docs returned (inclusive)
    // LinearRetrieverBuilder builderZeroMinScore = new LinearRetrieverBuilder(
    // List.of(
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverA, null),
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverB, null)
    // ),
    // rankWindowSize,
    // weights,
    // identityNormalizers
    // ).minScore(0.0f);

    // SearchSourceBuilder sourceZeroMinScore = new SearchSourceBuilder().retriever(builderZeroMinScore).size(rankWindowSize);

    // ElasticsearchAssertions.assertResponse(
    // client().prepareSearch(INDEX).setSource(sourceZeroMinScore),
    // resp -> assertThat(resp.getHits().getTotalHits().value(), equalTo(3L))
    // );

    // // Scenario 3: Basic filtering - minScore = 10.5f
    // LinearRetrieverBuilder builderFilterBasic = new LinearRetrieverBuilder(
    // List.of(
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverA, null),
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverB, null)
    // ),
    // rankWindowSize,
    // weights,
    // identityNormalizers
    // ).minScore(10.5f);

    // SearchSourceBuilder sourceFilterBasic = new SearchSourceBuilder().retriever(builderFilterBasic).size(rankWindowSize);

    // ElasticsearchAssertions.assertResponse(client().prepareSearch(INDEX).setSource(sourceFilterBasic), resp -> {
    // assertThat(resp.getHits().getTotalHits().value(), equalTo(2L)); // doc_2 and doc_3 have score 11.0
    // List<String> ids = Arrays.stream(resp.getHits().getHits()).map(h -> h.getId()).collect(Collectors.toList());
    // assertThat(ids, containsInAnyOrder("doc_2", "doc_3"));
    // });

    // // Scenario 4: Filter all documents - minScore = 20.0f
    // LinearRetrieverBuilder builderFilterAll = new LinearRetrieverBuilder(
    // List.of(
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverA, null),
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverB, null)
    // ),
    // rankWindowSize,
    // weights,
    // identityNormalizers
    // ).minScore(20.0f);

    // SearchSourceBuilder sourceFilterAll = new SearchSourceBuilder().retriever(builderFilterAll).size(rankWindowSize);

    // ElasticsearchAssertions.assertResponse(
    // client().prepareSearch(INDEX).setSource(sourceFilterAll),
    // resp -> assertThat(resp.getHits().getTotalHits().value(), equalTo(0L))
    // );

    // // Scenario 5: Test with MinMax normalization
    // StandardRetrieverBuilder retrieverC = new StandardRetrieverBuilder(QueryBuilders.termQuery(TEXT_FIELD, "term1").boost(4.0f));
    // StandardRetrieverBuilder retrieverD = new StandardRetrieverBuilder(QueryBuilders.termQuery(TEXT_FIELD, "term2").boost(1.0f));

    // ScoreNormalizer[] minMaxNormalizers = new ScoreNormalizer[] { MinMaxScoreNormalizer.INSTANCE, MinMaxScoreNormalizer.INSTANCE };

    // LinearRetrieverBuilder builderWithNorm = new LinearRetrieverBuilder(
    // List.of(
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverC, null),
    // new CompoundRetrieverBuilder.RetrieverSource(retrieverD, null)
    // ),
    // rankWindowSize,
    // weights,
    // minMaxNormalizers
    // ).minScore(1.1f);

    // SearchSourceBuilder sourceWithNorm = new SearchSourceBuilder().retriever(builderWithNorm).size(rankWindowSize);

    // ElasticsearchAssertions.assertResponse(client().prepareSearch(INDEX).setSource(sourceWithNorm), resp -> {
    // // With MinMax normalization, we expect doc_2 and doc_3 to have scores > 1.1
    // assertThat(resp.getHits().getTotalHits().value(), equalTo(2L));
    // List<String> ids = Arrays.stream(resp.getHits().getHits()).map(h -> h.getId()).collect(Collectors.toList());
    // assertThat(ids, containsInAnyOrder("doc_2", "doc_3"));
    // });
    // }
}
