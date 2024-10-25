/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.gaussDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.linearDecayFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class DecayFunctionScoreIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    private final QueryBuilder baseQuery = constantScoreQuery(termQuery("test", "value"));

    public void testDistanceScoreGeoLinGaussExp() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            prepareIndex("test").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 10)
                        .field("lon", 20)
                        .endObject()
                        .endObject()
                )
        );
        indexBuilders.add(
            prepareIndex("test").setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 22)
                        .endObject()
                        .endObject()
                )
        );

        int numDummyDocs = 20;
        for (int i = 1; i <= numDummyDocs; i++) {
            indexBuilders.add(
                prepareIndex("test").setId(Integer.toString(i + 3))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("test", "value")
                            .startObject("loc")
                            .field("lat", 11 + i)
                            .field("lon", 22 + i)
                            .endObject()
                            .endObject()
                    )
            );
        }

        indexRandom(true, indexBuilders);

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        assertHitCount(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(baseQuery))
            ),
            (numDummyDocs + 2)
        );

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(searchSource().query(functionScoreQuery(baseQuery, gaussDecayFunction("loc", lonlat, "1000km"))))
            ),
            response -> {
                assertHitCount(response, (numDummyDocs + 2));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            }
        );
        // Test Exp

        assertHitCount(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(baseQuery))
            ),
            (numDummyDocs + 2)
        );

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(searchSource().query(functionScoreQuery(baseQuery, linearDecayFunction("loc", lonlat, "1000km"))))
            ),
            response -> {
                assertHitCount(response, (numDummyDocs + 2));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            }
        );

        // Test Lin

        assertHitCount(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(baseQuery))
            ),
            (numDummyDocs + 2)
        );

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(searchSource().query(functionScoreQuery(baseQuery, exponentialDecayFunction("loc", lonlat, "1000km"))))
            ),
            response -> {
                assertHitCount(response, (numDummyDocs + 2));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            }
        );
    }

    public void testDistanceScoreGeoLinGaussExpWithOffset() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        // add tw docs within offset
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("test", "value").field("num", 0.5).endObject())
        );
        indexBuilders.add(
            prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.7).endObject())
        );

        // add docs outside offset
        int numDummyDocs = 20;
        for (int i = 0; i < numDummyDocs; i++) {
            indexBuilders.add(
                prepareIndex("test").setId(Integer.toString(i + 3))
                    .setSource(jsonBuilder().startObject().field("test", "value").field("num", 3.0 + i).endObject())
            );
        }

        indexRandom(true, indexBuilders);

        // Test Gauss

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().size(numDummyDocs + 2)
                            .query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 1.0, 5.0, 1.0)).boostMode(
                                    CombineFunction.REPLACE
                                )
                            )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));
                assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
                assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
                assertThat(sh.getAt(1).getScore(), equalTo(sh.getAt(0).getScore()));
                for (int i = 0; i < numDummyDocs; i++) {
                    assertThat(sh.getAt(i + 2).getId(), equalTo(Integer.toString(i + 3)));
                }
            }
        );

        // Test Exp

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().size(numDummyDocs + 2)
                            .query(
                                functionScoreQuery(termQuery("test", "value"), exponentialDecayFunction("num", 1.0, 5.0, 1.0)).boostMode(
                                    CombineFunction.REPLACE
                                )
                            )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));
                assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
                assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
                assertThat(sh.getAt(1).getScore(), equalTo(sh.getAt(0).getScore()));
                for (int i = 0; i < numDummyDocs; i++) {
                    assertThat(sh.getAt(i + 2).getId(), equalTo(Integer.toString(i + 3)));
                }
            }
        );
        // Test Lin
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().size(numDummyDocs + 2)
                            .query(
                                functionScoreQuery(termQuery("test", "value"), linearDecayFunction("num", 1.0, 20.0, 1.0)).boostMode(
                                    CombineFunction.REPLACE
                                )
                            )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));
                assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
                assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
                assertThat(sh.getAt(1).getScore(), equalTo(sh.getAt(0).getScore()));
            }
        );
    }

    public void testBoostModeSettingWorks() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).build();
        assertAcked(
            prepareCreate("test").setSettings(settings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("test")
                        .field("type", "text")
                        .endObject()
                        .startObject("loc")
                        .field("type", "geo_point")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            prepareIndex("test").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 21)
                        .endObject()
                        .endObject()
                )
        );
        indexBuilders.add(
            prepareIndex("test").setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 20)
                        .endObject()
                        .endObject()
                )
        );
        indexRandom(true, false, indexBuilders); // force no dummy docs

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                CombineFunction.MULTIPLY
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat(sh.getAt(1).getId(), equalTo("2"));
            }
        );
        // Test Exp
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(searchSource().query(termQuery("test", "value")))
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat(sh.getAt(1).getId(), equalTo("2"));
            }
        );

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
                assertThat(sh.getAt(0).getId(), equalTo("2"));
                assertThat(sh.getAt(1).getId(), equalTo("1"));
            }
        );

    }

    public void testParseGeoPoint() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startObject("loc")
                    .field("lat", 20)
                    .field("lon", 11)
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        FunctionScoreQueryBuilder baseQueryBuilder = functionScoreQuery(
            constantScoreQuery(termQuery("test", "value")),
            ScoreFunctionBuilders.weightFactorFunction(randomIntBetween(1, 10))
        );
        GeoPoint point = new GeoPoint(20, 11);
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("loc", point, "1000km")).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(1.0, 1.e-5));
            }
        );
        // this is equivalent to new GeoPoint(20, 11); just flipped so scores must be same
        float[] coords = { 11, 20 };
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("loc", coords, "1000km")).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(1.0f, 1.e-5));
            }
        );
    }

    public void testCombineModes() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        prepareIndex("test").setId("1")
            .setRefreshPolicy(IMMEDIATE)
            .setSource(jsonBuilder().startObject().field("test", "value value").field("num", 1.0).endObject())
            .get();
        FunctionScoreQueryBuilder baseQueryBuilder = functionScoreQuery(
            constantScoreQuery(termQuery("test", "value")),
            ScoreFunctionBuilders.weightFactorFunction(2)
        );
        // decay score should return 0.5 for this function and baseQuery should return 2.0f as it's score
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(
                                CombineFunction.MULTIPLY
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(1.0, 1.e-5));
            }
        );
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(0.5, 1.e-5));
            }
        );
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(
                                CombineFunction.SUM
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(2.0 + 0.5, 1.e-5));
                logger.info(
                    "--> Hit[0] {} Explanation:\n {}",
                    response.getHits().getAt(0).getId(),
                    response.getHits().getAt(0).getExplanation()
                );
            }
        );

        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(
                                CombineFunction.AVG
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo((2.0 + 0.5) / 2, 1.e-5));
            }
        );
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(
                                CombineFunction.MIN
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(0.5, 1.e-5));
            }
        );
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(baseQueryBuilder, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(
                                CombineFunction.MAX
                            )
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat((double) sh.getAt(0).getScore(), closeTo(2.0, 1.e-5));
            }
        );
    }

    public void testExceptionThrownIfScaleLE0() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(
            new IndexRequest("test").id("1")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())
        ).actionGet();
        client().index(
            new IndexRequest("test").id("2")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())
        ).actionGet();
        refresh();

        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "2013-05-28", "-1d"))
                        )
                    )
            )
        );
        assertThat(e.getMessage(), is("all shards failed"));
    }

    public void testParseDateMath() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .field("format", "epoch_millis")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(
            new IndexRequest("test").id("1")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", System.currentTimeMillis()).endObject())
        ).actionGet();
        client().index(
            new IndexRequest("test").id("2")
                .source(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .field("num1", System.currentTimeMillis() - (1000 * 60 * 60 * 24))
                        .endObject()
                )
        ).actionGet();
        refresh();

        assertNoFailuresAndResponse(
            client().search(
                new SearchRequest(new String[] {}).source(
                    searchSource().query(functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now", "2d")))
                )
            ),
            response -> assertOrderedSearchHits(response, "1", "2")
        );

        assertNoFailuresAndResponse(
            client().search(
                new SearchRequest(new String[] {}).source(
                    searchSource().query(functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now-1d", "2d")))
                )
            ),
            response -> assertOrderedSearchHits(response, "2", "1")
        );
    }

    public void testValueMissingLin() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .startObject("num2")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().index(
            new IndexRequest("test").id("1")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").field("num2", "1.0").endObject())
        ).actionGet();
        client().index(
            new IndexRequest("test").id("2").source(jsonBuilder().startObject().field("test", "value").field("num2", "1.0").endObject())
        ).actionGet();
        client().index(
            new IndexRequest("test").id("3")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").field("num2", "1.0").endObject())
        ).actionGet();
        client().index(
            new IndexRequest("test").id("4")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").endObject())
        ).actionGet();

        refresh();

        assertNoFailuresAndResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(
                                baseQuery,
                                new FilterFunctionBuilder[] {
                                    new FilterFunctionBuilder(linearDecayFunction("num1", "2013-05-28", "+3d")),
                                    new FilterFunctionBuilder(linearDecayFunction("num2", "0.0", "1")) }
                            ).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getHits().length, equalTo(4));
                double[] scores = new double[4];
                for (int i = 0; i < sh.getHits().length; i++) {
                    scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
                }
                assertThat(scores[0], lessThan(scores[1]));
                assertThat(scores[2], lessThan(scores[3]));
            }
        );
    }

    public void testDateWithoutOrigin() throws Exception {
        ZonedDateTime dt = ZonedDateTime.now(ZoneOffset.UTC);

        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ZonedDateTime docDate = dt.minusDays(1);
        String docDateString = docDate.getYear()
            + "-"
            + Strings.format("%02d", docDate.getMonthValue())
            + "-"
            + Strings.format("%02d", docDate.getDayOfMonth());
        client().index(
            new IndexRequest("test").id("1")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())
        ).actionGet();
        docDate = dt.minusDays(2);
        docDateString = docDate.getYear()
            + "-"
            + Strings.format("%02d", docDate.getMonthValue())
            + "-"
            + Strings.format("%02d", docDate.getDayOfMonth());
        client().index(
            new IndexRequest("test").id("2")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())
        ).actionGet();
        docDate = dt.minusDays(3);
        docDateString = docDate.getYear()
            + "-"
            + Strings.format("%02d", docDate.getMonthValue())
            + "-"
            + Strings.format("%02d", docDate.getDayOfMonth());
        client().index(
            new IndexRequest("test").id("3")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())
        ).actionGet();

        refresh();

        assertNoFailuresAndResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(
                                QueryBuilders.matchAllQuery(),
                                new FilterFunctionBuilder[] {
                                    new FilterFunctionBuilder(linearDecayFunction("num1", null, "7000d")),
                                    new FilterFunctionBuilder(gaussDecayFunction("num1", null, "1d")),
                                    new FilterFunctionBuilder(exponentialDecayFunction("num1", null, "7000d")) }
                            ).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                        )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getHits().length, equalTo(3));
                double[] scores = new double[4];
                for (int i = 0; i < sh.getHits().length; i++) {
                    scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
                }
                assertThat(scores[1], lessThan(scores[0]));
                assertThat(scores[2], lessThan(scores[1]));
            }
        );
    }

    public void testManyDocsLin() throws Exception {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("test")
            .field("type", "text")
            .endObject()
            .startObject("date")
            .field("type", "date")
            .field("doc_values", true)
            .endObject()
            .startObject("num")
            .field("type", "double")
            .field("doc_values", true)
            .endObject()
            .startObject("geo")
            .field("type", "geo_point")
            .field("ignore_malformed", true);
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        int numDocs = 200;
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            double lat = 100 + (int) (10.0 * (i) / (numDocs));
            double lon = 100;
            int day = (int) (29.0 * (i) / (numDocs)) + 1;
            String dayString = day < 10 ? "0" + Integer.toString(day) : Integer.toString(day);
            String date = "2013-05-" + dayString;

            indexBuilders.add(
                prepareIndex("test").setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("test", "value")
                            .field("date", date)
                            .field("num", i)
                            .startObject("geo")
                            .field("lat", lat)
                            .field("lon", lon)
                            .endObject()
                            .endObject()
                    )
            );
        }
        indexRandom(true, indexBuilders);
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(100f);
        lonlat.add(110f);
        assertNoFailuresAndResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().size(numDocs)
                            .query(
                                functionScoreQuery(
                                    termQuery("test", "value"),
                                    new FilterFunctionBuilder[] {
                                        new FilterFunctionBuilder(linearDecayFunction("date", "2013-05-30", "+15d")),
                                        new FilterFunctionBuilder(linearDecayFunction("geo", lonlat, "1000km")),
                                        new FilterFunctionBuilder(linearDecayFunction("num", numDocs, numDocs / 2.0)) }
                                ).scoreMode(ScoreMode.MULTIPLY).boostMode(CombineFunction.REPLACE)
                            )
                    )
            ),
            response -> {
                SearchHits sh = response.getHits();
                assertThat(sh.getHits().length, equalTo(numDocs));
                double[] scores = new double[numDocs];
                for (int i = 0; i < numDocs; i++) {
                    scores[Integer.parseInt(sh.getAt(i).getId())] = sh.getAt(i).getScore();
                }
                for (int i = 0; i < numDocs - 1; i++) {
                    assertThat(scores[i], lessThan(scores[i + 1]));
                }
            }
        );
    }

    public void testParsingExceptionIfFieldDoesNotExist() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("geo")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        int numDocs = 2;
        client().index(
            new IndexRequest("test").source(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startObject("geo")
                    .field("lat", 1)
                    .field("lon", 2)
                    .endObject()
                    .endObject()
            )
        ).actionGet();
        refresh();
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(100f);
        lonlat.add(110f);

        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().size(numDocs)
                            .query(
                                functionScoreQuery(termQuery("test", "value"), linearDecayFunction("type.geo", lonlat, "1000km")).scoreMode(
                                    FunctionScoreQuery.ScoreMode.MULTIPLY
                                )
                            )
                    )
            )
        );
        assertThat(e.getMessage(), is("all shards failed"));
    }

    public void testParsingExceptionIfFieldTypeDoesNotMatch() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(
            new IndexRequest("test").source(
                jsonBuilder().startObject().field("test", "value").field("num", Integer.toString(1)).endObject()
            )
        ).actionGet();
        refresh();
        // so, we indexed a string field, but now we try to score a num field
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(termQuery("test", "value"), linearDecayFunction("num", 1.0, 0.5)).scoreMode(
                                ScoreMode.MULTIPLY
                            )
                        )
                    )
            )
        );
        assertThat(e.getMessage(), is("all shards failed"));
    }

    public void testNoQueryGiven() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(new IndexRequest("test").source(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject()))
            .actionGet();
        refresh();
        // so, we indexed a string field, but now we try to score a num field
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).searchType(SearchType.QUERY_THEN_FETCH)
                    .source(
                        searchSource().query(
                            functionScoreQuery(linearDecayFunction("num", 1, 0.5)).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                        )
                    )
            ),
            response -> {}
        );
    }

    public void testMultiFieldOptions() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .startObject("num")
                    .field("type", "float")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        // Index for testing MIN and MAX
        IndexRequestBuilder doc1 = prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startArray("loc")
                    .startObject()
                    .field("lat", 10)
                    .field("lon", 20)
                    .endObject()
                    .startObject()
                    .field("lat", 12)
                    .field("lon", 23)
                    .endObject()
                    .endArray()
                    .endObject()
            );
        IndexRequestBuilder doc2 = prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startObject("loc")
                    .field("lat", 11)
                    .field("lon", 22)
                    .endObject()
                    .endObject()
            );

        indexRandom(true, doc1, doc2);

        assertResponse(client().search(new SearchRequest(new String[] {}).source(searchSource().query(baseQuery))), response -> {
            assertSearchHits(response, "1", "2");
            SearchHits sh = response.getHits();
            assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
        });

        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(10f);
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("loc", lonlat, "1000km").setMultiValueMode(MultiValueMode.MIN))
                    )
                )
            ),
            response -> {
                assertSearchHits(response, "1", "2");
                SearchHits sh = response.getHits();

                assertThat(sh.getAt(0).getId(), equalTo("1"));
                assertThat(sh.getAt(1).getId(), equalTo("2"));
            }
        );
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("loc", lonlat, "1000km").setMultiValueMode(MultiValueMode.MAX))
                    )
                )
            ),
            response -> {
                assertSearchHits(response, "1", "2");
                SearchHits sh = response.getHits();

                assertThat(sh.getAt(0).getId(), equalTo("2"));
                assertThat(sh.getAt(1).getId(), equalTo("1"));
            }
        );

        // Now test AVG and SUM

        doc1 = prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject().field("test", "value").startArray("num").value(0.0).value(1.0).value(2.0).endArray().endObject()
            );
        doc2 = prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject());

        indexRandom(true, doc1, doc2);
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, linearDecayFunction("num", "0", "10").setMultiValueMode(MultiValueMode.SUM))
                    )
                )
            ),
            response -> {
                assertSearchHits(response, "1", "2");
                SearchHits sh = response.getHits();

                assertThat(sh.getAt(0).getId(), equalTo("2"));
                assertThat(sh.getAt(1).getId(), equalTo("1"));
                assertThat(1.0 - sh.getAt(0).getScore(), closeTo((1.0 - sh.getAt(1).getScore()) / 3.0, 1.e-6d));
            }
        );
        assertResponse(
            client().search(
                new SearchRequest(new String[] {}).source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, linearDecayFunction("num", "0", "10").setMultiValueMode(MultiValueMode.AVG))
                    )
                )
            ),
            response -> {
                assertSearchHits(response, "1", "2");
                SearchHits sh = response.getHits();
                assertThat((double) (sh.getAt(0).getScore()), closeTo((sh.getAt(1).getScore()), 1.e-6d));
            }
        );
    }
}
