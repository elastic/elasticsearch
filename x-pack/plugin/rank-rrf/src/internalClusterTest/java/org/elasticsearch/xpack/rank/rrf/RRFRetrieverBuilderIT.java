/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.query.InnerHitBuilder;
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
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 3)
public class RRFRetrieverBuilderIT extends ESIntegTestCase {

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
                    "type": "hnsw"
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

    public void testRRFPagination() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
        final List<String> expectedDocIds = List.of("doc_2", "doc_6", "doc_7", "doc_1", "doc_3", "doc_4");
        final int totalDocs = expectedDocIds.size();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            int from = randomIntBetween(0, totalDocs - 1);
            int size = randomIntBetween(1, totalDocs - from);
            for (int docs_to_fetch = from; docs_to_fetch < totalDocs; docs_to_fetch += size) {
                SearchSourceBuilder source = new SearchSourceBuilder();
                source.from(docs_to_fetch);
                source.size(size);
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
                KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(
                    VECTOR_FIELD,
                    new float[] { 2.0f },
                    null,
                    10,
                    100,
                    null,
                    null
                );
                source.retriever(
                    new RRFRetrieverBuilder(
                        Arrays.asList(
                            new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                            new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                            new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                        ),
                        rankWindowSize,
                        rankConstant
                    )
                );
                SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
                int fDocs_to_fetch = docs_to_fetch;
                ElasticsearchAssertions.assertResponse(req, resp -> {
                    assertNull(resp.pointInTimeId());
                    assertNotNull(resp.getHits().getTotalHits());
                    assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
                    assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
                    assertThat(resp.getHits().getHits().length, lessThanOrEqualTo(size));
                    for (int k = 0; k < Math.min(size, resp.getHits().getHits().length); k++) {
                        assertThat(resp.getHits().getAt(k).getId(), equalTo(expectedDocIds.get(k + fDocs_to_fetch)));
                    }
                });
            }
        }
    }

    public void testRRFWithAggs() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                rankConstant
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

    public void testRRFWithCollapse() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                rankConstant
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
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_6"));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_7"));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(3).getInnerHits().get("a").getAt(0).getId(), equalTo("doc_4"));
            assertThat(resp.getHits().getAt(3).getInnerHits().get("a").getAt(1).getId(), equalTo("doc_3"));
            assertThat(resp.getHits().getAt(3).getInnerHits().get("a").getAt(2).getId(), equalTo("doc_1"));
        });
    }

    public void testRRFRetrieverWithCollapseAndAggs() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                rankConstant
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
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_7"));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(3).getInnerHits().get("a").getAt(0).getId(), equalTo("doc_4"));
            assertThat(resp.getHits().getAt(3).getInnerHits().get("a").getAt(1).getId(), equalTo("doc_3"));
            assertThat(resp.getHits().getAt(3).getInnerHits().get("a").getAt(2).getId(), equalTo("doc_1"));

            assertNotNull(resp.getAggregations());
            assertNotNull(resp.getAggregations().get("topic_agg"));
            Terms terms = resp.getAggregations().get("topic_agg");

            assertThat(terms.getBucketByKey("technology").getDocCount(), equalTo(3L));
            assertThat(terms.getBucketByKey("astronomy").getDocCount(), equalTo(1L));
            assertThat(terms.getBucketByKey("biology").getDocCount(), equalTo(1L));
        });
    }

    public void testMultipleRRFRetrievers() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(
                        // this one returns docs 6, 7, 1, 3, and 4
                        new RRFRetrieverBuilder(
                            Arrays.asList(
                                new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                                new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                                new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                            ),
                            rankWindowSize,
                            rankConstant
                        ),
                        null
                    ),
                    // this one bring just doc 7 which should be ranked first eventually
                    new CompoundRetrieverBuilder.RetrieverSource(
                        new KnnRetrieverBuilder(VECTOR_FIELD, new float[] { 7.0f }, null, 1, 100, null, null),
                        null
                    )
                ),
                rankWindowSize,
                rankConstant
            )
        );

        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value(), equalTo(6L));
            assertThat(resp.getHits().getTotalHits().relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_7"));
            assertThat(resp.getHits().getAt(1).getId(), equalTo("doc_2"));
            assertThat(resp.getHits().getAt(2).getId(), equalTo("doc_6"));
            assertThat(resp.getHits().getAt(3).getId(), equalTo("doc_1"));
            assertThat(resp.getHits().getAt(4).getId(), equalTo("doc_3"));
            assertThat(resp.getHits().getAt(5).getId(), equalTo("doc_4"));
        });
    }

    public void testRRFExplainWithNamedRetrievers() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
        standard0.retrieverName("my_custom_retriever");
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
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
                ),
                rankWindowSize,
                rankConstant
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
            assertThat(rrfDetails.getDescription(), containsString("computed for initial ranks [2, 1, 1]"));

            assertThat(rrfDetails.getDetails()[0].getDescription(), containsString("for rank [2] in query at index [0]"));
            assertThat(rrfDetails.getDetails()[0].getDescription(), containsString("[my_custom_retriever]"));
            assertThat(rrfDetails.getDetails()[1].getDescription(), containsString("for rank [1] in query at index [1]"));
            assertThat(rrfDetails.getDetails()[2].getDescription(), containsString("for rank [1] in query at index [2]"));
        });
    }

    public void testRRFExplainWithAnotherNestedRRF() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
        standard0.retrieverName("my_custom_retriever");
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

        RRFRetrieverBuilder nestedRRF = new RRFRetrieverBuilder(
            Arrays.asList(
                new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                new CompoundRetrieverBuilder.RetrieverSource(standard1, null),
                new CompoundRetrieverBuilder.RetrieverSource(knnRetrieverBuilder, null)
            ),
            rankWindowSize,
            rankConstant
        );
        StandardRetrieverBuilder standard2 = new StandardRetrieverBuilder(
            QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds("doc_6")).boost(20L)
        );
        source.retriever(
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(nestedRRF, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard2, null)
                ),
                rankWindowSize,
                rankConstant
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
            var rrfTopLevel = resp.getHits().getAt(0).getExplanation().getDetails()[0];
            assertThat(rrfTopLevel.getDetails().length, equalTo(2));
            assertThat(rrfTopLevel.getDescription(), containsString("computed for initial ranks [2, 1]"));
            assertThat(rrfTopLevel.getDetails()[0].getDetails()[0].getDescription(), containsString("rrf score"));
            assertThat(rrfTopLevel.getDetails()[1].getDetails()[0].getDescription(), containsString("ConstantScore"));

            var rrfDetails = rrfTopLevel.getDetails()[0].getDetails()[0];
            assertThat(rrfDetails.getDetails().length, equalTo(3));
            assertThat(rrfDetails.getDescription(), containsString("computed for initial ranks [4, 2, 3]"));

            assertThat(rrfDetails.getDetails()[0].getDescription(), containsString("for rank [4] in query at index [0]"));
            assertThat(rrfDetails.getDetails()[0].getDescription(), containsString("for rank [4] in query at index [0]"));
            assertThat(rrfDetails.getDetails()[0].getDescription(), containsString("[my_custom_retriever]"));
            assertThat(rrfDetails.getDetails()[1].getDescription(), containsString("for rank [2] in query at index [1]"));
            assertThat(rrfDetails.getDetails()[2].getDescription(), containsString("for rank [3] in query at index [2]"));
        });
    }

    public void testRRFInnerRetrieverAll4xxSearchErrors() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                ),
                rankWindowSize,
                rankConstant
            )
        );
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        Exception ex = expectThrows(ElasticsearchStatusException.class, req::get);
        assertThat(ex, instanceOf(ElasticsearchStatusException.class));
        assertThat(
            ex.getMessage(),
            containsString(
                "[rrf] search failed - retrievers '[standard]' returned errors. All failures are attached as suppressed exceptions."
            )
        );
        assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ex.getSuppressed().length, equalTo(1));
        assertThat(ex.getSuppressed()[0].getCause().getCause(), instanceOf(IllegalArgumentException.class));
    }

    public void testRRFInnerRetrieverMultipleErrorsOne5xx() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standard0, null),
                    new CompoundRetrieverBuilder.RetrieverSource(testRetrieverBuilder, null)
                ),
                rankWindowSize,
                rankConstant
            )
        );
        SearchRequestBuilder req = client().prepareSearch(INDEX).setSource(source);
        Exception ex = expectThrows(ElasticsearchStatusException.class, req::get);
        assertThat(ex, instanceOf(ElasticsearchStatusException.class));
        assertThat(
            ex.getMessage(),
            containsString(
                "[rrf] search failed - retrievers '[standard, test]' returned errors. All failures are attached as suppressed exceptions."
            )
        );
        assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(ex.getSuppressed().length, equalTo(2));
        assertThat(ex.getSuppressed()[0].getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(ex.getSuppressed()[1].getCause().getCause(), instanceOf(IllegalStateException.class));
    }

    public void testRRFInnerRetrieverErrorWhenExtractingToSource() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(failingRetriever, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                ),
                rankWindowSize,
                rankConstant
            )
        );
        source.size(1);
        expectThrows(UnsupportedOperationException.class, () -> client().prepareSearch(INDEX).setSource(source).get());
    }

    public void testRRFInnerRetrieverErrorOnTopDocs() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(failingRetriever, null),
                    new CompoundRetrieverBuilder.RetrieverSource(standard1, null)
                ),
                rankWindowSize,
                rankConstant
            )
        );
        source.size(1);
        source.aggregation(AggregationBuilders.terms("topic_agg").field(TOPIC_FIELD));
        expectThrows(UnsupportedOperationException.class, () -> client().prepareSearch(INDEX).setSource(source).get());
    }

    public void testRRFFiltersPropagatedToKnnQueryVectorBuilder() {
        final int rankWindowSize = 100;
        final int rankConstant = 10;
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
            new RRFRetrieverBuilder(
                Arrays.asList(
                    new CompoundRetrieverBuilder.RetrieverSource(standardRetriever, null),
                    new CompoundRetrieverBuilder.RetrieverSource(knnRetriever, null)
                ),
                rankWindowSize,
                rankConstant
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
        var rrf = new RRFRetrieverBuilder(
            List.of(new CompoundRetrieverBuilder.RetrieverSource(knn, null), new CompoundRetrieverBuilder.RetrieverSource(standard, null)),
            10,
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
}
