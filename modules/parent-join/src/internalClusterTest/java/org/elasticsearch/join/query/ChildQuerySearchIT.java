/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.join.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.parentId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ChildQuerySearchIT extends ParentChildTestCase {

    public void testMultiLevelChild() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child", "child", "grandchild")
            )
        );
        ensureGreen();

        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "c_value1").get();
        createIndexRequest("test", "grandchild", "gc1", "c1", "gc_field", "gc_value1").setRouting("p1").get();
        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery())
                    .filter(
                        hasChildQuery(
                            "child",
                            boolQuery().must(termQuery("c_field", "c_value1"))
                                .filter(hasChildQuery("grandchild", termQuery("gc_field", "gc_value1"), ScoreMode.None)),
                            ScoreMode.None
                        )
                    )
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", termQuery("p_field", "p_value1"), false))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("c1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasParentQuery("child", termQuery("c_field", "c_value1"), false))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("gc1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "p_value1"), false)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("c1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasParentQuery("child", termQuery("c_field", "c_value1"), false)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("gc1"));
            }
        );
    }

    // see #2744
    public void test2744() throws IOException {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "foo", "test")));
        ensureGreen();

        // index simple data
        createIndexRequest("test", "foo", "1", null, "foo", 1).get();
        createIndexRequest("test", "test", "2", "1", "foo", 1).get();
        refresh();
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("test", matchQuery("foo", 1), ScoreMode.None)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testSimpleChildQuery() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "yellow").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "blue").get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "red").get();
        refresh();

        // TEST FETCHING _parent from child
        assertNoFailuresAndResponse(prepareSearch("test").setQuery(idsQuery().addIds("c1")), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getAt(0).getId(), equalTo("c1"));
            assertThat(extractValue("join_field.name", response.getHits().getAt(0).getSourceAsMap()), equalTo("child"));
            assertThat(extractValue("join_field.parent", response.getHits().getAt(0).getSourceAsMap()), equalTo("p1"));

        });

        // TEST matching on parent
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().filter(termQuery("join_field#parent", "p1")).filter(termQuery("join_field", "child"))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("c1"), equalTo("c2")));
                assertThat(extractValue("join_field.name", response.getHits().getAt(0).getSourceAsMap()), equalTo("child"));
                assertThat(extractValue("join_field.parent", response.getHits().getAt(0).getSourceAsMap()), equalTo("p1"));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("c1"), equalTo("c2")));
                assertThat(extractValue("join_field.name", response.getHits().getAt(1).getSourceAsMap()), equalTo("child"));
                assertThat(extractValue("join_field.parent", response.getHits().getAt(1).getSourceAsMap()), equalTo("p1"));
            }
        );

        // HAS CHILD
        assertNoFailuresAndResponse(prepareSearch("test").setQuery(randomHasChild("child", "c_field", "yellow")), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
        });

        assertNoFailuresAndResponse(prepareSearch("test").setQuery(randomHasChild("child", "c_field", "blue")), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), equalTo("p2"));
        });

        assertNoFailuresAndResponse(prepareSearch("test").setQuery(randomHasChild("child", "c_field", "red")), response -> {
            assertHitCount(response, 2L);
            assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("p2"), equalTo("p1")));
            assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("p2"), equalTo("p1")));
        });

        // HAS PARENT
        assertNoFailuresAndResponse(prepareSearch("test").setQuery(randomHasParent("parent", "p_field", "p_value2")), response -> {
            assertHitCount(response, 2L);
            assertThat(response.getHits().getAt(0).getId(), equalTo("c3"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("c4"));
        });

        assertNoFailuresAndResponse(prepareSearch("test").setQuery(randomHasParent("parent", "p_field", "p_value1")), response -> {
            assertHitCount(response, 2L);
            assertThat(response.getHits().getAt(0).getId(), equalTo("c1"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("c2"));
        });
    }

    // Issue #3290
    public void testCachingBugWithFqueryFilter() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        // index simple data
        for (int i = 0; i < 10; i++) {
            builders.add(createIndexRequest("test", "parent", Integer.toString(i), null, "p_field", i));
        }
        indexRandom(randomBoolean(), builders);
        builders.clear();
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                builders.add(createIndexRequest("test", "child", j + "-" + i, "0", "c_field", i));
            }
            for (int i = 10; i < 20; i++) {
                builders.add(createIndexRequest("test", "child", j + "-" + i, Integer.toString(i), "c_field", i));
            }

            if (randomBoolean()) {
                break; // randomly break out and dont' have deletes / updates
            }
        }
        indexRandom(true, builders);

        for (int i = 1; i <= 10; i++) {
            logger.info("Round {}", i);
            assertNoFailures(prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.Max))));
            assertNoFailures(prepareSearch("test").setQuery(constantScoreQuery(hasParentQuery("parent", matchAllQuery(), true))));
        }
    }

    public void testHasParentFilter() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();
        Map<String, Set<String>> parentToChildren = new HashMap<>();
        // Childless parent
        createIndexRequest("test", "parent", "p0", null, "p_field", "p0").get();
        parentToChildren.put("p0", new HashSet<>());

        String previousParentId = null;
        int numChildDocs = 32;
        int numChildDocsPerParent = 0;
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 1; i <= numChildDocs; i++) {

            if (previousParentId == null || i % numChildDocsPerParent == 0) {
                previousParentId = "p" + i;
                builders.add(createIndexRequest("test", "parent", previousParentId, null, "p_field", previousParentId));
                numChildDocsPerParent++;
            }

            String childId = "c" + i;
            builders.add(createIndexRequest("test", "child", childId, previousParentId, "c_field", childId));

            if (parentToChildren.containsKey(previousParentId) == false) {
                parentToChildren.put(previousParentId, new HashSet<>());
            }
            assertThat(parentToChildren.get(previousParentId).add(childId), is(true));
        }
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));

        assertThat(parentToChildren.isEmpty(), equalTo(false));
        for (Map.Entry<String, Set<String>> parentToChildrenEntry : parentToChildren.entrySet()) {
            assertNoFailuresAndResponse(
                prepareSearch("test").setQuery(
                    constantScoreQuery(hasParentQuery("parent", termQuery("p_field", parentToChildrenEntry.getKey()), false))
                ).setSize(numChildDocsPerParent),
                response -> {
                    Set<String> childIds = parentToChildrenEntry.getValue();
                    assertThat(response.getHits().getTotalHits().value, equalTo((long) childIds.size()));
                    for (int i = 0; i < response.getHits().getTotalHits().value; i++) {
                        assertThat(childIds.remove(response.getHits().getAt(i).getId()), is(true));
                        assertThat(response.getHits().getAt(i).getScore(), is(1.0f));
                    }
                    assertThat(childIds.size(), is(0));
                }
            );
        }
    }

    public void testSimpleChildQueryWithFlush() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // index simple data with flushes, so we have many segments
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        indicesAdmin().prepareFlush().get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "red").get();
        indicesAdmin().prepareFlush().get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "yellow").get();
        indicesAdmin().prepareFlush().get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        indicesAdmin().prepareFlush().get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "blue").get();
        indicesAdmin().prepareFlush().get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "red").get();
        indicesAdmin().prepareFlush().get();
        refresh();

        // HAS CHILD QUERY
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"), ScoreMode.None)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"), ScoreMode.None)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p2"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"), ScoreMode.None)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("p2"), equalTo("p1")));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("p2"), equalTo("p1")));
            }
        );

        // HAS CHILD FILTER
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "yellow"), ScoreMode.None))),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"), ScoreMode.None))),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p2"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "red"), ScoreMode.None))),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("p2"), equalTo("p1")));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("p2"), equalTo("p1")));
            }
        );
    }

    public void testScopedFacet() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"), "c_field", "keyword")
            )
        );
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "yellow").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "blue").get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "red").get();

        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                hasChildQuery(
                    "child",
                    boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow")),
                    ScoreMode.None
                )
            )
                .addAggregation(
                    AggregationBuilders.global("global")
                        .subAggregation(
                            AggregationBuilders.filter(
                                "filter",
                                boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))
                            ).subAggregation(AggregationBuilders.terms("facet1").field("c_field"))
                        )
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), anyOf(equalTo("p2"), equalTo("p1")));
                assertThat(response.getHits().getAt(1).getId(), anyOf(equalTo("p2"), equalTo("p1")));

                Global global = response.getAggregations().get("global");
                Filter filter = global.getAggregations().get("filter");
                Terms termsFacet = filter.getAggregations().get("facet1");
                assertThat(termsFacet.getBuckets().size(), equalTo(2));
                assertThat(termsFacet.getBuckets().get(0).getKeyAsString(), equalTo("red"));
                assertThat(termsFacet.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(termsFacet.getBuckets().get(1).getKeyAsString(), equalTo("yellow"));
                assertThat(termsFacet.getBuckets().get(1).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testDeletedParent() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();
        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "yellow").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "blue").get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "red").get();

        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "yellow"), ScoreMode.None))),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
                assertThat(response.getHits().getAt(0).getSourceAsString(), containsString("\"p_value1\""));
            }
        );

        // update p1 and see what that we get updated values...

        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1_updated").get();
        indicesAdmin().prepareRefresh().get();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "yellow"), ScoreMode.None))),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
                assertThat(response.getHits().getAt(0).getSourceAsString(), containsString("\"p_value1_updated\""));
            }
        );
    }

    public void testDfsSearchType() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "yellow").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c3", "p3", "c_field", "blue").get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "red").get();

        refresh();

        assertNoFailures(
            prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasChildQuery("child", boolQuery().should(queryStringQuery("c_field:*")), ScoreMode.None)))
        );

        assertNoFailures(
            prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasParentQuery("parent", boolQuery().should(queryStringQuery("p_field:*")), false)))
        );
    }

    public void testHasChildAndHasParentFailWhenSomeSegmentsDontContainAnyParentOrChildDocs() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        createIndexRequest("test", "parent", "1", null, "p_field", 1).get();
        createIndexRequest("test", "child", "2", "1", "c_field", 1).get();

        prepareIndex("test").setId("3").setSource("p_field", 1).get();
        refresh();

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
            ),
            1L
        );

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", matchAllQuery(), false))),
            1L
        );
    }

    public void testCountApiUsage() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        String parentId = "p1";
        createIndexRequest("test", "parent", parentId, null, "p_field", "1").get();
        createIndexRequest("test", "child", "c1", parentId, "c_field", "1").get();
        refresh();

        assertHitCount(prepareSearch("test").setSize(0).setQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.Max)), 1L);

        assertHitCount(prepareSearch("test").setSize(0).setQuery(hasParentQuery("parent", termQuery("p_field", "1"), true)), 1L);

        assertHitCount(
            prepareSearch("test").setSize(0)
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.None))),
            1L
        );

        assertHitCount(
            prepareSearch("test").setSize(0).setQuery(constantScoreQuery(hasParentQuery("parent", termQuery("p_field", "1"), false))),
            1L
        );
    }

    public void testExplainUsage() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        String parentId = "p1";
        createIndexRequest("test", "parent", parentId, null, "p_field", "1").get();
        createIndexRequest("test", "child", "c1", parentId, "c_field", "1").get();
        refresh();

        assertResponse(
            prepareSearch("test").setExplain(true).setQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.Max)),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getExplanation().getDescription(), containsString("join value p1"));
            }
        );

        assertResponse(
            prepareSearch("test").setExplain(true).setQuery(hasParentQuery("parent", termQuery("p_field", "1"), true)),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getExplanation().getDescription(), containsString("join value p1"));
            }
        );

        ExplainResponse explainResponse = client().prepareExplain("test", parentId)
            .setQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.Max))
            .get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.getExplanation().toString(), containsString("join value p1"));
    }

    List<IndexRequestBuilder> createDocBuilders() {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        // Parent 1 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "1", null, "p_field", "p_value1"));
        indexBuilders.add(createIndexRequest("test", "child", "4", "1", "c_field1", 1, "c_field2", 0));
        indexBuilders.add(createIndexRequest("test", "child", "5", "1", "c_field1", 1, "c_field2", 0));
        indexBuilders.add(createIndexRequest("test", "child", "6", "1", "c_field1", 2, "c_field2", 0));
        indexBuilders.add(createIndexRequest("test", "child", "7", "1", "c_field1", 2, "c_field2", 0));
        indexBuilders.add(createIndexRequest("test", "child", "8", "1", "c_field1", 1, "c_field2", 1));
        indexBuilders.add(createIndexRequest("test", "child", "9", "1", "c_field1", 1, "c_field2", 2));

        // Parent 2 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "2", null, "p_field", "p_value2"));
        indexBuilders.add(createIndexRequest("test", "child", "10", "2", "c_field1", 3, "c_field2", 0));
        indexBuilders.add(createIndexRequest("test", "child", "11", "2", "c_field1", 1, "c_field2", 1));
        indexBuilders.add(createIndexRequest("test", "child", "12", "p", "c_field1", 1, "c_field2", 1)); // why "p"????
        indexBuilders.add(createIndexRequest("test", "child", "13", "2", "c_field1", 1, "c_field2", 1));
        indexBuilders.add(createIndexRequest("test", "child", "14", "2", "c_field1", 1, "c_field2", 1));
        indexBuilders.add(createIndexRequest("test", "child", "15", "2", "c_field1", 1, "c_field2", 2));

        // Parent 3 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "3", null, "p_field1", "p_value3", "p_field2", 5));
        indexBuilders.add(createIndexRequest("test", "child", "16", "3", "c_field1", 4, "c_field2", 0, "c_field3", 0));
        indexBuilders.add(createIndexRequest("test", "child", "17", "3", "c_field1", 1, "c_field2", 1, "c_field3", 1));
        indexBuilders.add(createIndexRequest("test", "child", "18", "3", "c_field1", 1, "c_field2", 2, "c_field3", 2));
        indexBuilders.add(createIndexRequest("test", "child", "19", "3", "c_field1", 1, "c_field2", 2, "c_field3", 3));
        indexBuilders.add(createIndexRequest("test", "child", "20", "3", "c_field1", 1, "c_field2", 2, "c_field3", 4));
        indexBuilders.add(createIndexRequest("test", "child", "21", "3", "c_field1", 1, "c_field2", 2, "c_field3", 5));
        indexBuilders.add(createIndexRequest("test", "child1", "22", "3", "c_field1", 1, "c_field2", 2, "c_field3", 6));

        return indexBuilders;
    }

    public void testScoreForParentChildQueriesWithFunctionScore() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("parent", new String[] { "child", "child1" })
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        indexRandom(true, createDocBuilders().toArray(new IndexRequestBuilder[0]));
        assertResponse(
            prepareSearch("test").setQuery(
                hasChildQuery(
                    "child",
                    QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), fieldValueFactorFunction("c_field1"))
                        .boostMode(CombineFunction.REPLACE),
                    ScoreMode.Total
                )
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("1"));
                assertThat(response.getHits().getHits()[0].getScore(), equalTo(6f));
                assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
                assertThat(response.getHits().getHits()[1].getScore(), equalTo(4f));
                assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
                assertThat(response.getHits().getHits()[2].getScore(), equalTo(3f));
            }
        );

        assertResponse(
            prepareSearch("test").setQuery(
                hasChildQuery(
                    "child",
                    QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), fieldValueFactorFunction("c_field1"))
                        .boostMode(CombineFunction.REPLACE),
                    ScoreMode.Max
                )
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
                assertThat(response.getHits().getHits()[0].getScore(), equalTo(4f));
                assertThat(response.getHits().getHits()[1].getId(), equalTo("2"));
                assertThat(response.getHits().getHits()[1].getScore(), equalTo(3f));
                assertThat(response.getHits().getHits()[2].getId(), equalTo("1"));
                assertThat(response.getHits().getHits()[2].getScore(), equalTo(2f));
            }
        );

        assertResponse(
            prepareSearch("test").setQuery(
                hasChildQuery(
                    "child",
                    QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), fieldValueFactorFunction("c_field1"))
                        .boostMode(CombineFunction.REPLACE),
                    ScoreMode.Avg
                )
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
                assertThat(response.getHits().getHits()[0].getScore(), equalTo(4f));
                assertThat(response.getHits().getHits()[1].getId(), equalTo("2"));
                assertThat(response.getHits().getHits()[1].getScore(), equalTo(3f));
                assertThat(response.getHits().getHits()[2].getId(), equalTo("1"));
                assertThat(response.getHits().getHits()[2].getScore(), equalTo(1.5f));
            }
        );

        assertResponse(
            prepareSearch("test").setQuery(
                hasParentQuery(
                    "parent",
                    QueryBuilders.functionScoreQuery(matchQuery("p_field1", "p_value3"), fieldValueFactorFunction("p_field2"))
                        .boostMode(CombineFunction.REPLACE),
                    true
                )
            ).addSort(SortBuilders.fieldSort("c_field3")).addSort(SortBuilders.scoreSort()),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(7L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("16"));
                assertThat(response.getHits().getHits()[0].getScore(), equalTo(5f));
                assertThat(response.getHits().getHits()[1].getId(), equalTo("17"));
                assertThat(response.getHits().getHits()[1].getScore(), equalTo(5f));
                assertThat(response.getHits().getHits()[2].getId(), equalTo("18"));
                assertThat(response.getHits().getHits()[2].getScore(), equalTo(5f));
                assertThat(response.getHits().getHits()[3].getId(), equalTo("19"));
                assertThat(response.getHits().getHits()[3].getScore(), equalTo(5f));
                assertThat(response.getHits().getHits()[4].getId(), equalTo("20"));
                assertThat(response.getHits().getHits()[4].getScore(), equalTo(5f));
                assertThat(response.getHits().getHits()[5].getId(), equalTo("21"));
                assertThat(response.getHits().getHits()[5].getScore(), equalTo(5f));
                assertThat(response.getHits().getHits()[6].getId(), equalTo("22"));
                assertThat(response.getHits().getHits()[6].getScore(), equalTo(5f));
            }
        );
    }

    // Issue #2536
    public void testParentChildQueriesCanHandleNoRelevantTypesInIndex() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(hasChildQuery("child", matchQuery("text", "value"), ScoreMode.None)),
            0L
        );

        prepareIndex("test").setSource(jsonBuilder().startObject().field("text", "value").endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(hasChildQuery("child", matchQuery("text", "value"), ScoreMode.None)),
            0L
        );

        assertHitCountAndNoFailures(prepareSearch("test").setQuery(hasChildQuery("child", matchQuery("text", "value"), ScoreMode.Max)), 0L);

        assertHitCountAndNoFailures(prepareSearch("test").setQuery(hasParentQuery("parent", matchQuery("text", "value"), false)), 0L);

        assertHitCountAndNoFailures(prepareSearch("test").setQuery(hasParentQuery("parent", matchQuery("text", "value"), true)), 0L);
    }

    public void testHasChildAndHasParentFilter_withFilter() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        createIndexRequest("test", "parent", "1", null, "p_field", 1).get();
        createIndexRequest("test", "child", "2", "1", "c_field", 1).get();
        indicesAdmin().prepareFlush("test").get();

        prepareIndex("test").setId("3").setSource("p_field", 2).get();

        refresh();
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", termQuery("c_field", 1), ScoreMode.None))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("1"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", termQuery("p_field", 1), false))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("2"));
            }
        );
    }

    public void testHasChildInnerHitsHighlighting() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        createIndexRequest("test", "parent", "1", null, "p_field", 1).get();
        createIndexRequest("test", "child", "2", "1", "c_field", "foo bar").get();
        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                hasChildQuery("child", matchQuery("c_field", "foo"), ScoreMode.None).innerHit(
                    new InnerHitBuilder().setHighlightBuilder(
                        new HighlightBuilder().field(new Field("c_field").highlightQuery(QueryBuilders.matchQuery("c_field", "bar")))
                    )
                )
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("1"));
                SearchHit[] searchHits = response.getHits().getHits()[0].getInnerHits().get("child").getHits();
                assertThat(searchHits.length, equalTo(1));
                HighlightField highlightField1 = searchHits[0].getHighlightFields().get("c_field");
                assertThat(highlightField1.fragments().length, equalTo(1));
                HighlightField highlightField = searchHits[0].getHighlightFields().get("c_field");
                assertThat(highlightField.fragments()[0].string(), equalTo("foo <em>bar</em>"));
            }
        );
    }

    public void testHasChildAndHasParentWrappedInAQueryFilter() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // query filter in case for p/c shouldn't execute per segment, but rather
        createIndexRequest("test", "parent", "1", null, "p_field", 1).get();
        indicesAdmin().prepareFlush("test").setForce(true).get();
        createIndexRequest("test", "child", "2", "1", "c_field", 1).get();
        refresh();

        assertResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchQuery("c_field", 1), ScoreMode.None))
            ),
            response -> assertSearchHit(response, 1, hasId("1"))
        );

        assertResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", matchQuery("p_field", 1), false))
            ),
            response -> assertSearchHit(response, 1, hasId("2"))
        );

        assertResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(boolQuery().must(hasChildQuery("child", matchQuery("c_field", 1), ScoreMode.None)))
            ),
            response -> assertSearchHit(response, 1, hasId("1"))
        );

        assertResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchAllQuery()).filter(boolQuery().must(hasParentQuery("parent", matchQuery("p_field", 1), false)))
            ),
            response -> assertSearchHit(response, 1, hasId("2"))
        );
    }

    public void testSimpleQueryRewrite() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                addFieldMappings(
                    buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"),
                    "c_field",
                    "keyword",
                    "p_field",
                    "keyword"
                )
            )
        );
        ensureGreen();

        // index simple data
        int childId = 0;
        for (int i = 0; i < 10; i++) {
            String parentId = Strings.format("p%03d", i);
            createIndexRequest("test", "parent", parentId, null, "p_field", parentId).get();
            int j = childId;
            for (; j < childId + 50; j++) {
                String childUid = Strings.format("c%03d", j);
                createIndexRequest("test", "child", childUid, parentId, "c_field", childUid).get();
            }
            childId = j;
        }
        refresh();

        SearchType[] searchTypes = new SearchType[] { SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH };
        for (SearchType searchType : searchTypes) {
            assertNoFailuresAndResponse(
                prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasChildQuery("child", prefixQuery("c_field", "c"), ScoreMode.Max))
                    .addSort("p_field", SortOrder.ASC)
                    .setSize(5),
                response -> {
                    assertThat(response.getHits().getTotalHits().value, equalTo(10L));
                    assertThat(response.getHits().getHits()[0].getId(), equalTo("p000"));
                    assertThat(response.getHits().getHits()[1].getId(), equalTo("p001"));
                    assertThat(response.getHits().getHits()[2].getId(), equalTo("p002"));
                    assertThat(response.getHits().getHits()[3].getId(), equalTo("p003"));
                    assertThat(response.getHits().getHits()[4].getId(), equalTo("p004"));
                }
            );

            assertNoFailuresAndResponse(
                prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasParentQuery("parent", prefixQuery("p_field", "p"), true))
                    .addSort("c_field", SortOrder.ASC)
                    .setSize(5),
                response -> {
                    assertThat(response.getHits().getTotalHits().value, equalTo(500L));
                    assertThat(response.getHits().getHits()[0].getId(), equalTo("c000"));
                    assertThat(response.getHits().getHits()[1].getId(), equalTo("c001"));
                    assertThat(response.getHits().getHits()[2].getId(), equalTo("c002"));
                    assertThat(response.getHits().getHits()[3].getId(), equalTo("c003"));
                    assertThat(response.getHits().getHits()[4].getId(), equalTo("c004"));
                }
            );
        }
    }

    // Issue #3144
    public void testReIndexingParentAndChildDocuments() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "yellow").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "x").get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "x").get();

        refresh();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"), ScoreMode.Total)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
                assertThat(response.getHits().getAt(0).getSourceAsString(), containsString("\"p_value1\""));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchQuery("c_field", "x")).must(hasParentQuery("parent", termQuery("p_field", "p_value2"), true))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("c3"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("c4"));
            }
        );

        // re-index
        for (int i = 0; i < 10; i++) {
            createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
            createIndexRequest("test", "child", "d" + i, "p1", "c_field", "red").get();
            createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
            createIndexRequest("test", "child", "c3", "p2", "c_field", "x").get();
            indicesAdmin().prepareRefresh("test").get();
        }

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"), ScoreMode.Total)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p1"));
                assertThat(response.getHits().getAt(0).getSourceAsString(), containsString("\"p_value1\""));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchQuery("c_field", "x")).must(hasParentQuery("parent", termQuery("p_field", "p_value2"), true))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
                assertThat(response.getHits().getAt(1).getId(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
            }
        );
    }

    // Issue #3203
    public void testHasChildQueryWithMinimumScore() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "x").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "x").get();
        createIndexRequest("test", "child", "c4", "p2", "c_field", "x").get();
        createIndexRequest("test", "child", "c5", "p2", "c_field", "x").get();
        refresh();
        // Score needs to be 3 or above!
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.Total)).setMinScore(3),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("p2"));
                assertThat(response.getHits().getAt(0).getScore(), equalTo(3.0f));
            }
        );
    }

    public void testParentFieldQuery() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.refresh_interval", -1))
                .setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"))
        );
        ensureGreen();

        assertHitCount(
            prepareSearch("test").setQuery(
                boolQuery().filter(termQuery("join_field#parent", "p1")).filter(termQuery("join_field", "child"))
            ),
            0L
        );

        createIndexRequest("test", "child", "c1", "p1").get();
        refresh();

        assertHitCount(
            prepareSearch("test").setQuery(
                boolQuery().filter(termQuery("join_field#parent", "p1")).filter(termQuery("join_field", "child"))
            ),
            1L
        );

        createIndexRequest("test", "child", "c2", "p2").get();
        refresh();
        assertHitCount(
            prepareSearch("test").setQuery(
                boolQuery().should(boolQuery().filter(termQuery("join_field#parent", "p1")).filter(termQuery("join_field", "child")))
                    .should(boolQuery().filter(termQuery("join_field#parent", "p2")).filter(termQuery("join_field", "child")))
            ),
            2L
        );
    }

    public void testParentIdQuery() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1))
                .setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"))
        );
        ensureGreen();

        createIndexRequest("test", "child", "c1", "p1").get();
        refresh();

        assertHitCount(prepareSearch("test").setQuery(parentId("child", "p1")), 1L);

        createIndexRequest("test", "child", "c2", "p2").get();
        refresh();

        assertHitCount(prepareSearch("test").setQuery(boolQuery().should(parentId("child", "p1")).should(parentId("child", "p2"))), 2L);
    }

    public void testHasChildNotBeingCached() throws IOException {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "parent", "p3", null, "p_field", "p_value3").get();
        createIndexRequest("test", "parent", "p4", null, "p_field", "p_value4").get();
        createIndexRequest("test", "parent", "p5", null, "p_field", "p_value5").get();
        createIndexRequest("test", "parent", "p6", null, "p_field", "p_value6").get();
        createIndexRequest("test", "parent", "p7", null, "p_field", "p_value7").get();
        createIndexRequest("test", "parent", "p8", null, "p_field", "p_value8").get();
        createIndexRequest("test", "parent", "p9", null, "p_field", "p_value9").get();
        createIndexRequest("test", "parent", "p10", null, "p_field", "p_value10").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "blue").get();
        indicesAdmin().prepareFlush("test").get();
        indicesAdmin().prepareRefresh("test").get();

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"), ScoreMode.None))),
            1L
        );

        createIndexRequest("test", "child", "c2", "p2", "c_field", "blue").get();
        indicesAdmin().prepareRefresh("test").get();

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"), ScoreMode.None))),
            2L
        );
    }

    private QueryBuilder randomHasChild(String type, String field, String value) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return constantScoreQuery(hasChildQuery(type, termQuery(field, value), ScoreMode.None));
            } else {
                return boolQuery().must(matchAllQuery()).filter(hasChildQuery(type, termQuery(field, value), ScoreMode.None));
            }
        } else {
            return hasChildQuery(type, termQuery(field, value), ScoreMode.None);
        }
    }

    private QueryBuilder randomHasParent(String type, String field, String value) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return constantScoreQuery(hasParentQuery(type, termQuery(field, value), false));
            } else {
                return boolQuery().must(matchAllQuery()).filter(hasParentQuery(type, termQuery(field, value), false));
            }
        } else {
            return hasParentQuery(type, termQuery(field, value), false);
        }
    }

    // Issue #3818
    public void testHasChildQueryOnlyReturnsSingleChildType() throws Exception {
        assertAcked(
            prepareCreate("grandissue").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("grandparent", "parent")
                    .field("parent", new String[] { "child_type_one", "child_type_two" })
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        createIndexRequest("grandissue", "grandparent", "1", null, "name", "Grandpa").get();
        createIndexRequest("grandissue", "parent", "2", "1", "name", "Dana").get();
        createIndexRequest("grandissue", "child_type_one", "3", "2", "name", "William").setRouting("1").get();
        createIndexRequest("grandissue", "child_type_two", "4", "2", "name", "Kate").setRouting("1").get();
        refresh();

        assertHitCount(
            prepareSearch("grandissue").setQuery(
                boolQuery().must(
                    hasChildQuery(
                        "parent",
                        boolQuery().must(
                            hasChildQuery("child_type_one", boolQuery().must(queryStringQuery("name:William*")), ScoreMode.None)
                        ),
                        ScoreMode.None
                    )
                )
            ),
            1L
        );

        assertHitCount(
            prepareSearch("grandissue").setQuery(
                boolQuery().must(
                    hasChildQuery(
                        "parent",
                        boolQuery().must(
                            hasChildQuery("child_type_two", boolQuery().must(queryStringQuery("name:William*")), ScoreMode.None)
                        ),
                        ScoreMode.None
                    )
                )
            ),
            0L
        );
    }

    public void testHasChildQueryWithNestedInnerObjects() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"), "objects", "nested")
            )
        );
        ensureGreen();

        createIndexRequest(
            "test",
            "parent",
            "p1",
            null,
            jsonBuilder().startObject()
                .field("p_field", "1")
                .startArray("objects")
                .startObject()
                .field("i_field", "1")
                .endObject()
                .startObject()
                .field("i_field", "2")
                .endObject()
                .startObject()
                .field("i_field", "3")
                .endObject()
                .startObject()
                .field("i_field", "4")
                .endObject()
                .startObject()
                .field("i_field", "5")
                .endObject()
                .startObject()
                .field("i_field", "6")
                .endObject()
                .endArray()
                .endObject()
        ).get();
        createIndexRequest(
            "test",
            "parent",
            "p2",
            null,
            jsonBuilder().startObject()
                .field("p_field", "2")
                .startArray("objects")
                .startObject()
                .field("i_field", "1")
                .endObject()
                .startObject()
                .field("i_field", "2")
                .endObject()
                .endArray()
                .endObject()
        ).get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "blue").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "red").get();
        refresh();

        ScoreMode scoreMode = randomFrom(ScoreMode.values());
        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(
                boolQuery().must(hasChildQuery("child", termQuery("c_field", "blue"), scoreMode))
                    .filter(boolQuery().mustNot(termQuery("p_field", "3")))
            ),
            1L
        );

        assertHitCountAndNoFailures(
            prepareSearch("test").setQuery(
                boolQuery().must(hasChildQuery("child", termQuery("c_field", "red"), scoreMode))
                    .filter(boolQuery().mustNot(termQuery("p_field", "3")))
            ),
            2L
        );
    }

    public void testNamedFilters() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        String parentId = "p1";
        createIndexRequest("test", "parent", parentId, null, "p_field", "1").get();
        createIndexRequest("test", "child", "c1", parentId, "c_field", "1").get();
        refresh();

        assertResponse(
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.Max).queryName("test")),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));
            }
        );

        assertResponse(
            prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "1"), true).queryName("test")),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));
            }
        );

        assertResponse(
            prepareSearch("test").setQuery(
                constantScoreQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.None).queryName("test"))
            ),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));
            }
        );

        assertResponse(
            prepareSearch("test").setQuery(
                constantScoreQuery(hasParentQuery("parent", termQuery("p_field", "1"), false).queryName("test"))
            ),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));
            }
        );
    }

    public void testParentChildQueriesNoParentType() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1)));
        ensureGreen();

        String parentId = "p1";
        prepareIndex("test").setId(parentId).setSource("p_field", "1").get();
        refresh();

        try {
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.None)).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.Max)).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            prepareSearch("test").setPostFilter(hasChildQuery("child", termQuery("c_field", "1"), ScoreMode.None)).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "1"), true)).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            prepareSearch("test").setPostFilter(hasParentQuery("parent", termQuery("p_field", "1"), false)).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    public void testParentChildCaching() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.refresh_interval", -1))
                .setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"))
        );
        ensureGreen();

        // index simple data
        createIndexRequest("test", "parent", "p1", null, "p_field", "p_value1").get();
        createIndexRequest("test", "parent", "p2", null, "p_field", "p_value2").get();
        createIndexRequest("test", "child", "c1", "p1", "c_field", "blue").get();
        createIndexRequest("test", "child", "c2", "p1", "c_field", "red").get();
        createIndexRequest("test", "child", "c3", "p2", "c_field", "red").get();
        indicesAdmin().prepareForceMerge("test").setMaxNumSegments(1).setFlush(true).get();
        createIndexRequest("test", "parent", "p3", null, "p_field", "p_value3").get();
        createIndexRequest("test", "parent", "p4", null, "p_field", "p_value4").get();
        createIndexRequest("test", "child", "c4", "p3", "c_field", "green").get();
        createIndexRequest("test", "child", "c5", "p3", "c_field", "blue").get();
        createIndexRequest("test", "child", "c6", "p4", "c_field", "blue").get();
        indicesAdmin().prepareFlush("test").get();
        indicesAdmin().prepareRefresh("test").get();

        for (int i = 0; i < 2; i++) {
            assertHitCount(
                prepareSearch().setQuery(
                    boolQuery().must(matchAllQuery())
                        .filter(
                            boolQuery().must(hasChildQuery("child", matchQuery("c_field", "red"), ScoreMode.None)).must(matchAllQuery())
                        )
                ),
                2L
            );
        }

        createIndexRequest("test", "child", "c3", "p2", "c_field", "blue").get();
        indicesAdmin().prepareRefresh("test").get();

        assertHitCount(
            prepareSearch().setQuery(
                boolQuery().must(matchAllQuery())
                    .filter(boolQuery().must(hasChildQuery("child", matchQuery("c_field", "red"), ScoreMode.None)).must(matchAllQuery()))
            ),
            1L
        );
    }

    public void testParentChildQueriesViaScrollApi() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            createIndexRequest("test", "parent", "p" + i, null).get();
            createIndexRequest("test", "child", "c" + i, "p" + i).get();
        }

        refresh();

        QueryBuilder[] queries = new QueryBuilder[] {
            hasChildQuery("child", matchAllQuery(), ScoreMode.None),
            boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchAllQuery(), ScoreMode.None)),
            hasParentQuery("parent", matchAllQuery(), false),
            boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", matchAllQuery(), false)) };

        for (QueryBuilder query : queries) {
            assertScrollResponsesAndHitCount(
                client(),
                TimeValue.timeValueSeconds(60),
                prepareSearch("test").setScroll(TimeValue.timeValueSeconds(30)).setSize(1).addStoredField("_id").setQuery(query),
                10,
                (respNum, response) -> {
                    assertNoFailures(response);
                    assertThat(response.getHits().getTotalHits().value, equalTo(10L));
                }
            );
        }
    }

    private List<IndexRequestBuilder> createMinMaxDocBuilders() {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        // Parent 1 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "1", null, "id", 1));
        indexBuilders.add(createIndexRequest("test", "child", "10", "1", "foo", "one"));

        // Parent 2 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "2", null, "id", 2));
        indexBuilders.add(createIndexRequest("test", "child", "11", "2", "foo", "one"));
        indexBuilders.add(createIndexRequest("test", "child", "12", "2", "foo", "one two"));

        // Parent 3 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "3", null, "id", 3));
        indexBuilders.add(createIndexRequest("test", "child", "13", "3", "foo", "one"));
        indexBuilders.add(createIndexRequest("test", "child", "14", "3", "foo", "one two"));
        indexBuilders.add(createIndexRequest("test", "child", "15", "3", "foo", "one two three"));

        // Parent 4 and its children
        indexBuilders.add(createIndexRequest("test", "parent", "4", null, "id", 4));
        indexBuilders.add(createIndexRequest("test", "child", "16", "4", "foo", "one"));
        indexBuilders.add(createIndexRequest("test", "child", "17", "4", "foo", "one two"));
        indexBuilders.add(createIndexRequest("test", "child", "18", "4", "foo", "one two three"));
        indexBuilders.add(createIndexRequest("test", "child", "19", "4", "foo", "one two three four"));

        return indexBuilders;
    }

    private SearchRequestBuilder minMaxQuery(ScoreMode scoreMode, int minChildren, Integer maxChildren)
        throws SearchPhaseExecutionException {
        HasChildQueryBuilder hasChildQuery = hasChildQuery(
            "child",
            QueryBuilders.functionScoreQuery(
                constantScoreQuery(QueryBuilders.termQuery("foo", "two")),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(weightFactorFunction(1)),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("foo", "three"), weightFactorFunction(1)),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("foo", "four"), weightFactorFunction(1)) }
            ).boostMode(CombineFunction.REPLACE).scoreMode(FunctionScoreQuery.ScoreMode.SUM),
            scoreMode
        ).minMaxChildren(minChildren, maxChildren != null ? maxChildren : HasChildQueryBuilder.DEFAULT_MAX_CHILDREN);

        return prepareSearch("test").setQuery(hasChildQuery).addSort("_score", SortOrder.DESC).addSort("id", SortOrder.ASC);
    }

    public void testMinMaxChildren() throws Exception {
        assertAcked(prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        ensureGreen();

        indexRandom(true, createMinMaxDocBuilders().toArray(new IndexRequestBuilder[0]));

        // Score mode = NONE
        assertResponse(minMaxQuery(ScoreMode.None, 1, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.None, 2, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.None, 3, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
        });

        assertHitCount(minMaxQuery(ScoreMode.None, 4, null), 0L);

        assertResponse(minMaxQuery(ScoreMode.None, 1, 4), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.None, 1, 3), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.None, 1, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.None, 2, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1f));
        });

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> minMaxQuery(ScoreMode.None, 3, 2));
        assertThat(e.getMessage(), equalTo("[has_child] 'max_children' is less than 'min_children'"));

        // Score mode = SUM
        assertResponse(minMaxQuery(ScoreMode.Total, 1, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(6f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Total, 2, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(6f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(3f));
        });

        assertResponse(minMaxQuery(ScoreMode.Total, 3, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(6f));
        });

        assertHitCount(minMaxQuery(ScoreMode.Total, 4, null), 0L);

        assertResponse(minMaxQuery(ScoreMode.Total, 1, 4), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(6f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Total, 1, 3), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(6f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Total, 1, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Total, 2, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
        });

        e = expectThrows(IllegalArgumentException.class, () -> minMaxQuery(ScoreMode.Total, 3, 2));
        assertThat(e.getMessage(), equalTo("[has_child] 'max_children' is less than 'min_children'"));

        // Score mode = MAX
        assertResponse(minMaxQuery(ScoreMode.Max, 1, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Max, 2, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(2f));
        });

        assertResponse(minMaxQuery(ScoreMode.Max, 3, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
        });

        assertHitCount(minMaxQuery(ScoreMode.Max, 4, null), 0L);

        assertResponse(minMaxQuery(ScoreMode.Max, 1, 4), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Max, 1, 3), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(3f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Max, 1, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Max, 2, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
        });

        e = expectThrows(IllegalArgumentException.class, () -> minMaxQuery(ScoreMode.Max, 3, 2));
        assertThat(e.getMessage(), equalTo("[has_child] 'max_children' is less than 'min_children'"));

        // Score mode = AVG
        assertResponse(minMaxQuery(ScoreMode.Avg, 1, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1.5f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Avg, 2, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1.5f));
        });

        assertResponse(minMaxQuery(ScoreMode.Avg, 3, null), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
        });

        assertHitCount(minMaxQuery(ScoreMode.Avg, 4, null), 0L);

        assertResponse(minMaxQuery(ScoreMode.Avg, 1, 4), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1.5f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Avg, 1, 3), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("4"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(2f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1.5f));
            assertThat(response.getHits().getHits()[2].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[2].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Avg, 1, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1.5f));
            assertThat(response.getHits().getHits()[1].getId(), equalTo("2"));
            assertThat(response.getHits().getHits()[1].getScore(), equalTo(1f));
        });

        assertResponse(minMaxQuery(ScoreMode.Avg, 2, 2), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("3"));
            assertThat(response.getHits().getHits()[0].getScore(), equalTo(1.5f));
        });

        e = expectThrows(IllegalArgumentException.class, () -> minMaxQuery(ScoreMode.Avg, 3, 2));
        assertThat(e.getMessage(), equalTo("[has_child] 'max_children' is less than 'min_children'"));
    }

    public void testHasParentInnerQueryType() {
        assertAcked(
            prepareCreate("test").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent-type", "child-type"))
        );
        createIndexRequest("test", "child-type", "child-id", "parent-id").get();
        createIndexRequest("test", "parent-type", "parent-id", null).get();
        refresh();

        // make sure that when we explicitly set a type, the inner query is executed in the context of the child type instead
        assertSearchHits(
            prepareSearch("test").setQuery(hasChildQuery("child-type", new IdsQueryBuilder().addIds("child-id"), ScoreMode.None)),
            "parent-id"
        );
        // make sure that when we explicitly set a type, the inner query is executed in the context of the parent type instead
        assertSearchHits(
            prepareSearch("test").setQuery(hasParentQuery("parent-type", new IdsQueryBuilder().addIds("parent-id"), false)),
            "child-id"
        );
    }

    public void testHighlightersIgnoreParentChild() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("parent-type", "child-type")
                    .endObject()
                    .endObject()
                    .startObject("searchText")
                    .field("type", "text")
                    .field("term_vector", "with_positions_offsets")
                    .field("index_options", "offsets")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        createIndexRequest("test", "parent-type", "parent-id", null, "searchText", "quick brown fox").get();
        createIndexRequest("test", "child-type", "child-id", "parent-id", "searchText", "quick brown fox").get();
        refresh();

        String[] highlightTypes = new String[] { "plain", "fvh", "unified" };
        for (String highlightType : highlightTypes) {
            logger.info("Testing with highlight type [{}]", highlightType);
            assertResponse(
                prepareSearch("test").setQuery(
                    new BoolQueryBuilder().must(new MatchQueryBuilder("searchText", "fox"))
                        .must(new HasChildQueryBuilder("child-type", new MatchAllQueryBuilder(), ScoreMode.None))
                ).highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("searchText").highlighterType(highlightType))),
                response -> {
                    assertHitCount(response, 1);
                    assertThat(response.getHits().getAt(0).getId(), equalTo("parent-id"));
                    HighlightField highlightField = response.getHits().getAt(0).getHighlightFields().get("searchText");
                    assertThat(highlightField.fragments()[0].string(), equalTo("quick brown <em>fox</em>"));
                }
            );

            assertResponse(
                prepareSearch("test").setQuery(
                    new BoolQueryBuilder().must(new MatchQueryBuilder("searchText", "fox"))
                        .must(new HasParentQueryBuilder("parent-type", new MatchAllQueryBuilder(), false))
                ).highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("searchText").highlighterType(highlightType))),
                response -> {
                    assertHitCount(response, 1);
                    assertThat(response.getHits().getAt(0).getId(), equalTo("child-id"));
                    HighlightField highlightField = response.getHits().getAt(0).getHighlightFields().get("searchText");
                    assertThat(highlightField.fragments()[0].string(), equalTo("quick brown <em>fox</em>"));
                }
            );
        }
    }

    public void testAliasesFilterWithHasChildQuery() throws Exception {
        assertAcked(
            prepareCreate("my-index").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"))
        );
        createIndexRequest("my-index", "parent", "1", null).get();
        createIndexRequest("my-index", "child", "2", "1").get();
        refresh();

        assertAcked(
            indicesAdmin().prepareAliases().addAlias("my-index", "filter1", hasChildQuery("child", matchAllQuery(), ScoreMode.None))
        );
        assertAcked(indicesAdmin().prepareAliases().addAlias("my-index", "filter2", hasParentQuery("parent", matchAllQuery(), false)));

        assertResponse(prepareSearch("filter1"), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        });
        assertResponse(prepareSearch("filter2"), response -> {
            assertHitCount(response, 1L);
            assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
        });
    }

}
