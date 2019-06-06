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
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.SuiteScopeTestCase()
public class TopHitsIT extends ESIntegTestCase {

    private static final String TERMS_AGGS_FIELD = "terms";
    private static final String SORT_FIELD = "sort";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("5", script -> "5");
        }
    }

    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    static int numArticles;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", TERMS_AGGS_FIELD, "type=keyword"));
        assertAcked(prepareCreate("field-collapsing").addMapping("type", "group", "type=keyword"));
        createIndex("empty");
        assertAcked(prepareCreate("articles").addMapping("article",
            jsonBuilder().startObject().startObject("article").startObject("properties")
                .startObject(TERMS_AGGS_FIELD)
                    .field("type", "keyword")
                .endObject()
                .startObject("comments")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("user")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("date")
                            .field("type", "long")
                        .endObject()
                        .startObject("message")
                            .field("type", "text")
                            .field("store", true)
                            .field("term_vector", "with_positions_offsets")
                            .field("index_options", "offsets")
                            .endObject()
                        .startObject("reviewers")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("name")
                                    .field("type", "keyword")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()));
        ensureGreen("idx", "empty", "articles");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            builders.add(client().prepareIndex("idx", "type", Integer.toString(i)).setSource(jsonBuilder()
                    .startObject()
                    .field(TERMS_AGGS_FIELD, "val" + (i / 10))
                    .field(SORT_FIELD, i + 1)
                    .field("text", "some text to entertain")
                    .field("field1", 5)
                    .endObject()));
        }

        builders.add(client().prepareIndex("field-collapsing", "type", "1").setSource(jsonBuilder()
                .startObject()
                .field("group", "a")
                .field("text", "term x y z b")
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "2").setSource(jsonBuilder()
                .startObject()
                .field("group", "a")
                .field("text", "term x y z n rare")
                .field("value", 1)
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "3").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "x y z term")
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "4").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "x y term")
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "5").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "x term")
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "6").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "term rare")
                .field("value", 3)
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "7").setSource(jsonBuilder()
                .startObject()
                .field("group", "c")
                .field("text", "x y z term")
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "8").setSource(jsonBuilder()
                .startObject()
                .field("group", "c")
                .field("text", "x y term b")
                .endObject()));
        builders.add(client().prepareIndex("field-collapsing", "type", "9").setSource(jsonBuilder()
                .startObject()
                .field("group", "c")
                .field("text", "rare x term")
                .field("value", 2)
                .endObject()));

        numArticles = scaledRandomIntBetween(10, 100);
        numArticles -= (numArticles % 5);
        for (int i = 0; i < numArticles; i++) {
            XContentBuilder builder = randomFrom(jsonBuilder(), yamlBuilder(), smileBuilder());
            builder.startObject().field("date", i).startArray("comments");
            for (int j = 0; j < i; j++) {
                String user = Integer.toString(j);
                builder.startObject().field("id", j).field("user", user).field("message", "some text").endObject();
            }
            builder.endArray().endObject();

            builders.add(
                    client().prepareIndex("articles", "article").setSource(builder)
            );
        }

        builders.add(
                client().prepareIndex("articles", "article", "1")
                        .setSource(jsonBuilder().startObject().field("title", "title 1").field("body", "some text").startArray("comments")
                                .startObject()
                                    .field("user", "a").field("date", 1L).field("message", "some comment")
                                    .startArray("reviewers")
                                        .startObject().field("name", "user a").endObject()
                                        .startObject().field("name", "user b").endObject()
                                        .startObject().field("name", "user c").endObject()
                                    .endArray()
                                .endObject()
                                .startObject()
                                    .field("user", "b").field("date", 2L).field("message", "some other comment")
                                    .startArray("reviewers")
                                        .startObject().field("name", "user c").endObject()
                                        .startObject().field("name", "user d").endObject()
                                        .startObject().field("name", "user e").endObject()
                                    .endArray()
                                .endObject()
                                .endArray().endObject())
        );
        builders.add(
                client().prepareIndex("articles", "article", "2")
                        .setSource(jsonBuilder().startObject().field("title", "title 2").field("body", "some different text")
                            .startArray("comments")
                                .startObject()
                                    .field("user", "b").field("date", 3L).field("message", "some comment")
                                    .startArray("reviewers")
                                        .startObject().field("name", "user f").endObject()
                                    .endArray()
                                .endObject()
                                .startObject()
                                    .field("user", "c")
                                    .field("date", 4L)
                                    .field("message", "some other comment")
                                .endObject()
                            .endArray().endObject())
        );

        indexRandom(true, builders);
        ensureSearchable();
    }

    private String key(Terms.Bucket bucket) {
        return bucket.getKeyAsString();
    }

    public void testBasics() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(TERMS_AGGS_FIELD)
                        .subAggregation(
                                topHits("hits").sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                        )
                )
                .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        long higestSortValue = 0;
        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(10L));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getTotalHits().value, equalTo(10L));
            assertThat(hits.getHits().length, equalTo(3));
            higestSortValue += 10;
            assertThat((Long) hits.getAt(0).getSortValues()[0], equalTo(higestSortValue));
            assertThat((Long) hits.getAt(1).getSortValues()[0], equalTo(higestSortValue - 1));
            assertThat((Long) hits.getAt(2).getSortValues()[0], equalTo(higestSortValue - 2));

            assertThat(hits.getAt(0).getSourceAsMap().size(), equalTo(4));
        }
    }

    public void testIssue11119() throws Exception {
        // Test that top_hits aggregation is fed scores if query results size=0
        SearchResponse response = client()
                .prepareSearch("field-collapsing")
                .setSize(0)
                .setQuery(matchQuery("text", "x y z"))
                .addAggregation(terms("terms")
                    .executionHint(randomExecutionHint()).field("group").subAggregation(topHits("hits")))
                .get();

        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits().value, equalTo(8L));
        assertThat(response.getHits().getHits().length, equalTo(0));
        assertThat(response.getHits().getMaxScore(), equalTo(Float.NaN));
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));

        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            float bestScore = Float.MAX_VALUE;
            for (int h = 0; h < hits.getHits().length; h++) {
                float score=hits.getAt(h).getScore();
                assertThat(score, lessThanOrEqualTo(bestScore));
                assertThat(score, greaterThan(0f));
                bestScore = hits.getAt(h).getScore();
            }
        }

        // Also check that min_score setting works when size=0
        // (technically not a test of top_hits but implementation details are
        // tied up with the need to feed scores into the agg tree even when
        // users don't want ranked set of query results.)
        response = client()
                .prepareSearch("field-collapsing")
                .setSize(0)
                .setMinScore(0.0001f)
                .setQuery(matchQuery("text", "x y z"))
                .addAggregation(terms("terms").executionHint(randomExecutionHint()).field("group"))
                .get();

        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits().value, equalTo(8L));
        assertThat(response.getHits().getHits().length, equalTo(0));
        assertThat(response.getHits().getMaxScore(), equalTo(Float.NaN));
        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));
    }


    public void testBreadthFirstWithScoreNeeded() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .collectMode(SubAggCollectionMode.BREADTH_FIRST)
                        .field(TERMS_AGGS_FIELD)
                        .subAggregation(topHits("hits").size(3))
                ).get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(10L));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getTotalHits().value, equalTo(10L));
            assertThat(hits.getHits().length, equalTo(3));

            assertThat(hits.getAt(0).getSourceAsMap().size(), equalTo(4));
        }
    }

    public void testBreadthFirstWithAggOrderAndScoreNeeded() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .collectMode(SubAggCollectionMode.BREADTH_FIRST)
                .field(TERMS_AGGS_FIELD)
                .order(BucketOrder.aggregation("max", false))
                .subAggregation(max("max").field(SORT_FIELD))
                .subAggregation(topHits("hits").size(3))
            ).get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));
        int id = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + id));
            assertThat(bucket.getDocCount(), equalTo(10L));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getTotalHits().value, equalTo(10L));
            assertThat(hits.getHits().length, equalTo(3));

            assertThat(hits.getAt(0).getSourceAsMap().size(), equalTo(4));
            id--;
        }
    }

    public void testBasicsGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(topHits("hits"))).get();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        TopHits topHits = global.getAggregations().get("hits");
        assertThat(topHits, notNullValue());
        assertThat(topHits.getName(), equalTo("hits"));
        assertThat((TopHits) ((InternalAggregation)global).getProperty("hits"), sameInstance(topHits));

    }

    public void testPagination() throws Exception {
        int size = randomIntBetween(1, 10);
        int from = randomIntBetween(0, 10);
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .subAggregation(
                                        topHits("hits")
                                            .sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                                                .from(from)
                                                .size(size)
                                )
                )
                .get();
        assertSearchResponse(response);

        SearchResponse control = client().prepareSearch("idx")
                .setFrom(from)
                .setSize(size)
                .setPostFilter(QueryBuilders.termQuery(TERMS_AGGS_FIELD, "val0"))
                .addSort(SORT_FIELD, SortOrder.DESC)
                .get();
        assertSearchResponse(control);
        SearchHits controlHits = control.getHits();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        Terms.Bucket bucket = terms.getBucketByKey("val0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(10L));
        TopHits topHits = bucket.getAggregations().get("hits");
        SearchHits hits = topHits.getHits();
        assertThat(hits.getTotalHits().value, equalTo(controlHits.getTotalHits().value));
        assertThat(hits.getHits().length, equalTo(controlHits.getHits().length));
        for (int i = 0; i < hits.getHits().length; i++) {
            logger.info("{}: top_hits: [{}][{}] control: [{}][{}]", i, hits.getAt(i).getId(), hits.getAt(i).getSortValues()[0],
                controlHits.getAt(i).getId(), controlHits.getAt(i).getSortValues()[0]);
            assertThat(hits.getAt(i).getId(), equalTo(controlHits.getAt(i).getId()));
            assertThat(hits.getAt(i).getSortValues()[0], equalTo(controlHits.getAt(i).getSortValues()[0]));
        }
    }

    public void testSortByBucket() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .order(BucketOrder.aggregation("max_sort", false))
                                .subAggregation(
                                        topHits("hits").sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC)).trackScores(true)
                                )
                                .subAggregation(
                                        max("max_sort").field(SORT_FIELD)
                                )
                )
                .get();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        long higestSortValue = 50;
        int currentBucket = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(key(bucket), equalTo("val" + currentBucket--));
            assertThat(bucket.getDocCount(), equalTo(10L));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getTotalHits().value, equalTo(10L));
            assertThat(hits.getHits().length, equalTo(3));
            assertThat(hits.getAt(0).getSortValues()[0], equalTo(higestSortValue));
            assertThat(hits.getAt(1).getSortValues()[0], equalTo(higestSortValue - 1));
            assertThat(hits.getAt(2).getSortValues()[0], equalTo(higestSortValue - 2));
            Max max = bucket.getAggregations().get("max_sort");
            assertThat(max.getValue(), equalTo(((Long) higestSortValue).doubleValue()));
            higestSortValue -= 10;
        }
    }

    public void testFieldCollapsing() throws Exception {
        SearchResponse response = client()
                .prepareSearch("field-collapsing")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(matchQuery("text", "term rare"))
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field("group")
                                .order(BucketOrder.aggregation("max_score", false)).subAggregation(topHits("hits").size(1))
                                .subAggregation(max("max_score").field("value"))).get();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));

        Iterator<? extends Terms.Bucket> bucketIterator = terms.getBuckets().iterator();
        Terms.Bucket bucket = bucketIterator.next();
        assertThat(key(bucket), equalTo("b"));
        TopHits topHits = bucket.getAggregations().get("hits");
        SearchHits hits = topHits.getHits();
        assertThat(hits.getTotalHits().value, equalTo(4L));
        assertThat(hits.getHits().length, equalTo(1));
        assertThat(hits.getAt(0).getId(), equalTo("6"));

        bucket = bucketIterator.next();
        assertThat(key(bucket), equalTo("c"));
        topHits = bucket.getAggregations().get("hits");
        hits = topHits.getHits();
        assertThat(hits.getTotalHits().value, equalTo(3L));
        assertThat(hits.getHits().length, equalTo(1));
        assertThat(hits.getAt(0).getId(), equalTo("9"));

        bucket = bucketIterator.next();
        assertThat(key(bucket), equalTo("a"));
        topHits = bucket.getAggregations().get("hits");
        hits = topHits.getHits();
        assertThat(hits.getTotalHits().value, equalTo(2L));
        assertThat(hits.getHits().length, equalTo(1));
        assertThat(hits.getAt(0).getId(), equalTo("2"));
    }

    public void testFetchFeatures() {
        final boolean seqNoAndTerm = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
                .setQuery(matchQuery("text", "text").queryName("test"))
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .subAggregation(
                                        topHits("hits").size(1)
                                            .highlighter(new HighlightBuilder().field("text"))
                                            .explain(true)
                                            .storedField("text")
                                            .docValueField("field1")
                                            .scriptField("script",
                                                new Script(ScriptType.INLINE, MockScriptEngine.NAME, "5", Collections.emptyMap()))
                                            .fetchSource("text", null)
                                            .version(true)
                                            .seqNoAndPrimaryTerm(seqNoAndTerm)
                                )
                )
                .get();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (Terms.Bucket bucket : terms.getBuckets()) {
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getTotalHits().value, equalTo(10L));
            assertThat(hits.getHits().length, equalTo(1));

            SearchHit hit = hits.getAt(0);
            HighlightField highlightField = hit.getHighlightFields().get("text");
            assertThat(highlightField.getFragments().length, equalTo(1));
            assertThat(highlightField.getFragments()[0].string(), equalTo("some <em>text</em> to entertain"));

            Explanation explanation = hit.getExplanation();
            assertThat(explanation.toString(), containsString("text:text"));

            long version = hit.getVersion();
            assertThat(version, equalTo(1L));

            if (seqNoAndTerm) {
                assertThat(hit.getSeqNo(), greaterThanOrEqualTo(0L));
                assertThat(hit.getPrimaryTerm(), greaterThanOrEqualTo(1L));
            } else {
                assertThat(hit.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                assertThat(hit.getPrimaryTerm(), equalTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
            }

            assertThat(hit.getMatchedQueries()[0], equalTo("test"));

            DocumentField field = hit.field("field1");
            assertThat(field.getValue().toString(), equalTo("5"));

            assertThat(hit.getSourceAsMap().get("text").toString(), equalTo("some text to entertain"));

            field = hit.field("script");
            assertThat(field.getValue().toString(), equalTo("5"));

            assertThat(hit.getSourceAsMap().size(), equalTo(1));
            assertThat(hit.getSourceAsMap().get("text").toString(), equalTo("some text to entertain"));
        }
    }

    public void testInvalidSortField() throws Exception {
        try {
            client().prepareSearch("idx")
                    .addAggregation(terms("terms")
                                    .executionHint(randomExecutionHint())
                                    .field(TERMS_AGGS_FIELD)
                                    .subAggregation(
                                            topHits("hits").sort(SortBuilders.fieldSort("xyz").order(SortOrder.DESC))
                                    )
                    ).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("No mapping found for [xyz] in order to sort on"));
        }
    }

    public void testEmptyIndex() throws Exception {
        SearchResponse response = client().prepareSearch("empty")
                .addAggregation(topHits("hits"))
                .get();
        assertSearchResponse(response);

        TopHits hits = response.getAggregations().get("hits");
        assertThat(hits, notNullValue());
        assertThat(hits.getName(), equalTo("hits"));
        assertThat(hits.getHits().getTotalHits().value, equalTo(0L));
    }

    public void testTrackScores() throws Exception {
        boolean[] trackScores = new boolean[]{true, false};
        for (boolean trackScore : trackScores) {
            logger.info("Track score={}", trackScore);
            SearchResponse response = client().prepareSearch("field-collapsing")
                    .setQuery(matchQuery("text", "term rare"))
                    .addAggregation(terms("terms")
                                    .field("group")
                                    .subAggregation(
                                            topHits("hits")
                                                    .trackScores(trackScore)
                                                    .size(1)
                                                    .sort("_index", SortOrder.DESC)
                                    )
                    )
                    .get();
            assertSearchResponse(response);

            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("terms"));
            assertThat(terms.getBuckets().size(), equalTo(3));

            Terms.Bucket bucket = terms.getBucketByKey("a");
            assertThat(key(bucket), equalTo("a"));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getMaxScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
            assertThat(hits.getAt(0).getScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));

            bucket = terms.getBucketByKey("b");
            assertThat(key(bucket), equalTo("b"));
            topHits = bucket.getAggregations().get("hits");
            hits = topHits.getHits();
            assertThat(hits.getMaxScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
            assertThat(hits.getAt(0).getScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));

            bucket = terms.getBucketByKey("c");
            assertThat(key(bucket), equalTo("c"));
            topHits = bucket.getAggregations().get("hits");
            hits = topHits.getHits();
            assertThat(hits.getMaxScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
            assertThat(hits.getAt(0).getScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
        }
    }

    public void testTopHitsInNestedSimple() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("articles")
                .setQuery(matchQuery("title", "title"))
                .addAggregation(
                        nested("to-comments", "comments")
                                .subAggregation(
                                        terms("users")
                                                .field("comments.user")
                                                .subAggregation(
                                                        topHits("top-comments").sort("comments.date", SortOrder.ASC)
                                                )
                                )
                )
                .get();

        Nested nested = searchResponse.getAggregations().get("to-comments");
        assertThat(nested.getDocCount(), equalTo(4L));

        Terms terms = nested.getAggregations().get("users");
        Terms.Bucket bucket = terms.getBucketByKey("a");
        assertThat(bucket.getDocCount(), equalTo(1L));
        TopHits topHits = bucket.getAggregations().get("top-comments");
        SearchHits searchHits = topHits.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo(1L));
        assertThat(searchHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(searchHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(extractValue("date", searchHits.getAt(0).getSourceAsMap()), equalTo(1));

        bucket = terms.getBucketByKey("b");
        assertThat(bucket.getDocCount(), equalTo(2L));
        topHits = bucket.getAggregations().get("top-comments");
        searchHits = topHits.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo(2L));
        assertThat(searchHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(searchHits.getAt(0).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(extractValue("date", searchHits.getAt(0).getSourceAsMap()), equalTo(2));
        assertThat(searchHits.getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(searchHits.getAt(1).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(extractValue("date", searchHits.getAt(1).getSourceAsMap()), equalTo(3));

        bucket = terms.getBucketByKey("c");
        assertThat(bucket.getDocCount(), equalTo(1L));
        topHits = bucket.getAggregations().get("top-comments");
        searchHits = topHits.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo(1L));
        assertThat(searchHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(searchHits.getAt(0).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(extractValue("date", searchHits.getAt(0).getSourceAsMap()), equalTo(4));
    }

    public void testTopHitsInSecondLayerNested() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("articles")
                .setQuery(matchQuery("title", "title"))
                .addAggregation(
                        nested("to-comments", "comments")
                                .subAggregation(
                                    nested("to-reviewers", "comments.reviewers").subAggregation(
                                            // Also need to sort on _doc because there are two reviewers with the same name
                                            topHits("top-reviewers")
                                                .sort("comments.reviewers.name", SortOrder.ASC).sort("_doc", SortOrder.DESC).size(7)
                                    )
                                )
                                .subAggregation(topHits("top-comments").sort("comments.date", SortOrder.DESC).size(4))
                ).get();
        assertNoFailures(searchResponse);

        Nested toComments = searchResponse.getAggregations().get("to-comments");
        assertThat(toComments.getDocCount(), equalTo(4L));

        TopHits topComments = toComments.getAggregations().get("top-comments");
        assertThat(topComments.getHits().getTotalHits().value, equalTo(4L));
        assertThat(topComments.getHits().getHits().length, equalTo(4));

        assertThat(topComments.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(topComments.getHits().getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topComments.getHits().getAt(0).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(topComments.getHits().getAt(0).getNestedIdentity().getChild(), nullValue());

        assertThat(topComments.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(topComments.getHits().getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topComments.getHits().getAt(1).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(topComments.getHits().getAt(1).getNestedIdentity().getChild(), nullValue());

        assertThat(topComments.getHits().getAt(2).getId(), equalTo("1"));
        assertThat(topComments.getHits().getAt(2).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topComments.getHits().getAt(2).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(topComments.getHits().getAt(2).getNestedIdentity().getChild(), nullValue());

        assertThat(topComments.getHits().getAt(3).getId(), equalTo("1"));
        assertThat(topComments.getHits().getAt(3).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topComments.getHits().getAt(3).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(topComments.getHits().getAt(3).getNestedIdentity().getChild(), nullValue());

        Nested toReviewers = toComments.getAggregations().get("to-reviewers");
        assertThat(toReviewers.getDocCount(), equalTo(7L));

        TopHits topReviewers = toReviewers.getAggregations().get("top-reviewers");
        assertThat(topReviewers.getHits().getTotalHits().value, equalTo(7L));
        assertThat(topReviewers.getHits().getHits().length, equalTo(7));

        assertThat(topReviewers.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(0).getSourceAsMap()), equalTo("user a"));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));

        assertThat(topReviewers.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(1).getSourceAsMap()), equalTo("user b"));
        assertThat(topReviewers.getHits().getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(1).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(topReviewers.getHits().getAt(1).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(1).getNestedIdentity().getChild().getOffset(), equalTo(1));

        assertThat(topReviewers.getHits().getAt(2).getId(), equalTo("1"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(2).getSourceAsMap()), equalTo("user c"));
        assertThat(topReviewers.getHits().getAt(2).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(2).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(topReviewers.getHits().getAt(2).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(2).getNestedIdentity().getChild().getOffset(), equalTo(0));

        assertThat(topReviewers.getHits().getAt(3).getId(), equalTo("1"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(3).getSourceAsMap()), equalTo("user c"));
        assertThat(topReviewers.getHits().getAt(3).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(3).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(topReviewers.getHits().getAt(3).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(3).getNestedIdentity().getChild().getOffset(), equalTo(2));

        assertThat(topReviewers.getHits().getAt(4).getId(), equalTo("1"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(4).getSourceAsMap()), equalTo("user d"));
        assertThat(topReviewers.getHits().getAt(4).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(4).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(topReviewers.getHits().getAt(4).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(4).getNestedIdentity().getChild().getOffset(), equalTo(1));

        assertThat(topReviewers.getHits().getAt(5).getId(), equalTo("1"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(5).getSourceAsMap()), equalTo("user e"));
        assertThat(topReviewers.getHits().getAt(5).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(5).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(topReviewers.getHits().getAt(5).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(5).getNestedIdentity().getChild().getOffset(), equalTo(2));

        assertThat(topReviewers.getHits().getAt(6).getId(), equalTo("2"));
        assertThat(extractValue("name", topReviewers.getHits().getAt(6).getSourceAsMap()), equalTo("user f"));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("reviewers"));
        assertThat(topReviewers.getHits().getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));
    }

    public void testNestedFetchFeatures() {
        String hlType = randomFrom("plain", "fvh", "unified");
        HighlightBuilder.Field hlField = new HighlightBuilder.Field("comments.message")
                .highlightQuery(matchQuery("comments.message", "comment"))
                .forceSource(randomBoolean()) // randomly from stored field or _source
                .highlighterType(hlType);

        SearchResponse searchResponse = client()
                .prepareSearch("articles")
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "comment").queryName("test"), ScoreMode.Avg))
                .addAggregation(
                        nested("to-comments", "comments").subAggregation(
                                topHits("top-comments").size(1).highlighter(new HighlightBuilder().field(hlField)).explain(true)
                                                .docValueField("comments.user")
                                        .scriptField("script",
                                            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "5", Collections.emptyMap()))
                                                .fetchSource("comments.message", null)
                                        .version(true).sort("comments.date", SortOrder.ASC))).get();
        assertHitCount(searchResponse, 2);
        Nested nested = searchResponse.getAggregations().get("to-comments");
        assertThat(nested.getDocCount(), equalTo(4L));

        SearchHits hits = ((TopHits) nested.getAggregations().get("top-comments")).getHits();
        assertThat(hits.getTotalHits().value, equalTo(4L));
        SearchHit searchHit = hits.getAt(0);
        assertThat(searchHit.getId(), equalTo("1"));
        assertThat(searchHit.getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(searchHit.getNestedIdentity().getOffset(), equalTo(0));

        HighlightField highlightField = searchHit.getHighlightFields().get("comments.message");
        assertThat(highlightField.getFragments().length, equalTo(1));
        assertThat(highlightField.getFragments()[0].string(), equalTo("some <em>comment</em>"));

        // Can't explain nested hit with the main query, since both are in a different scopes, also the nested doc may not
        // even have matched with the main query.
        // If top_hits would have a query option then we can explain that query
        Explanation explanation = searchHit.getExplanation();
        assertFalse(explanation.isMatch());

        // Returns the version of the root document. Nested docs don't have a separate version
        long version = searchHit.getVersion();
        assertThat(version, equalTo(1L));

        assertThat(searchHit.getMatchedQueries(), arrayContaining("test"));

        DocumentField field = searchHit.field("comments.user");
        assertThat(field.getValue().toString(), equalTo("a"));

        field = searchHit.field("script");
        assertThat(field.getValue().toString(), equalTo("5"));

        assertThat(searchHit.getSourceAsMap().size(), equalTo(1));
        assertThat(extractValue("message", searchHit.getSourceAsMap()), equalTo("some comment"));
    }

    public void testTopHitsInNested() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("articles")
                .addAggregation(
                        histogram("dates")
                                .field("date")
                                .interval(5)
                                .subAggregation(
                                        nested("to-comments", "comments")
                                                .subAggregation(topHits("comments")
                                                        .highlighter(
                                                            new HighlightBuilder()
                                                                .field(
                                                                    new HighlightBuilder.Field("comments.message")
                                                                        .highlightQuery(matchQuery("comments.message", "text"))
                                                                )
                                                        )
                                                        .sort("comments.id", SortOrder.ASC))
                                )
                )
                .get();

        Histogram histogram = searchResponse.getAggregations().get("dates");
        for (int i = 0; i < numArticles; i += 5) {
            Histogram.Bucket bucket = histogram.getBuckets().get(i / 5);
            assertThat(bucket.getDocCount(), equalTo(5L));

            long numNestedDocs = 10 + (5 * i);
            Nested nested = bucket.getAggregations().get("to-comments");
            assertThat(nested.getDocCount(), equalTo(numNestedDocs));

            TopHits hits = nested.getAggregations().get("comments");
            SearchHits searchHits = hits.getHits();
            assertThat(searchHits.getTotalHits().value, equalTo(numNestedDocs));
            for (int j = 0; j < 3; j++) {
                assertThat(searchHits.getAt(j).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(searchHits.getAt(j).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(extractValue("id", searchHits.getAt(j).getSourceAsMap()), equalTo(0));

                HighlightField highlightField = searchHits.getAt(j).getHighlightFields().get("comments.message");
                assertThat(highlightField.getFragments().length, equalTo(1));
                assertThat(highlightField.getFragments()[0].string(), equalTo("some <em>text</em>"));
            }
        }
    }

    public void testUseMaxDocInsteadOfSize() throws Exception {
        client().admin().indices().prepareUpdateSettings("idx")
            .setSettings(Collections.singletonMap(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), ArrayUtil.MAX_ARRAY_LENGTH))
            .get();
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .subAggregation(
                                        topHits("hits").size(ArrayUtil.MAX_ARRAY_LENGTH - 1)
                                            .sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                                )
                )
                .get();
        assertNoFailures(response);
        client().admin().indices().prepareUpdateSettings("idx")
            .setSettings(Collections.singletonMap(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), null))
            .get();
    }

    public void testTooHighResultWindow() throws Exception {
        SearchResponse response = client()
            .prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD)
                .subAggregation(
                    topHits("hits").from(50).size(10).sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                )
            )
            .get();
        assertNoFailures(response);

        Exception e = expectThrows(SearchPhaseExecutionException.class, () -> client().prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD)
                .subAggregation(
                    topHits("hits").from(100).size(10).sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                )
            ).get());
        assertThat(e.getCause().getMessage(),
            containsString("the top hits aggregator [hits]'s from + size must be less than or equal to: [100] but was [110]"));
        e = expectThrows(SearchPhaseExecutionException.class, () -> client().prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD)
                .subAggregation(
                    topHits("hits").from(10).size(100).sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                )
            ).get());
        assertThat(e.getCause().getMessage(),
            containsString("the top hits aggregator [hits]'s from + size must be less than or equal to: [100] but was [110]"));

        client().admin().indices().prepareUpdateSettings("idx")
            .setSettings(Collections.singletonMap(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 110))
            .get();
        response = client().prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD)
                .subAggregation(
                    topHits("hits").from(100).size(10).sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                )).get();
        assertNoFailures(response);
        response = client().prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD)
                .subAggregation(
                    topHits("hits").from(10).size(100).sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                )).get();
        assertNoFailures(response);
        client().admin().indices().prepareUpdateSettings("idx")
            .setSettings(Collections.singletonMap(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), null))
            .get();
    }

    public void testNoStoredFields() throws Exception {
        SearchResponse response = client()
            .prepareSearch("idx")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(TERMS_AGGS_FIELD)
                .subAggregation(
                    topHits("hits").storedField("_none_")
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(10L));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.getTotalHits().value, equalTo(10L));
            assertThat(hits.getHits().length, equalTo(3));
            for (SearchHit hit : hits) {
                assertThat(hit.getSourceAsMap(), nullValue());
                assertThat(hit.getId(), nullValue());
                assertThat(hit.getType(), equalTo("type"));
            }
        }
    }

    /**
     * Make sure that a request using a script does not get cached and a request
     * not using a script does get cached.
     */
    public void testDontCacheScripts() throws Exception {
        try {
            assertAcked(prepareCreate("cache_test_idx").addMapping("type", "d", "type=long")
                .setSettings(
                    Settings.builder()
                        .put("requests.cache.enable", true)
                        .put("number_of_shards", 1)
                        .put("number_of_replicas", 1))
                .get());
            indexRandom(true, client().prepareIndex("cache_test_idx", "type", "1").setSource("s", 1),
                client().prepareIndex("cache_test_idx", "type", "2").setSource("s", 2));

            // Make sure we are starting with a clear cache
            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

            // Test that a request using a script field does not get cached
            SearchResponse r = client().prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(topHits("foo").scriptField("bar",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "5", Collections.emptyMap()))).get();
            assertSearchResponse(r);

            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

            // Test that a request using a script sort does not get cached
            r = client().prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(topHits("foo").sort(
                    SortBuilders.scriptSort(
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "5", Collections.emptyMap()), ScriptSortType.STRING)))
                .get();
            assertSearchResponse(r);

            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

            // To make sure that the cache is working test that a request not using
            // a script is cached
            r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(topHits("foo")).get();
            assertSearchResponse(r);

            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
            assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(1L));
        } finally {
            assertAcked(client().admin().indices().prepareDelete("cache_test_idx")); // delete this - if we use tests.iters it would fail
        }
    }

    public void testWithRescore() {
        // Rescore with default sort on relevancy (score)
        {
            SearchResponse response = client()
                .prepareSearch("idx")
                .addRescorer(
                        new QueryRescorerBuilder(new MatchAllQueryBuilder().boost(3.0f))
                )
                .addAggregation(terms("terms")
                    .field(TERMS_AGGS_FIELD)
                    .subAggregation(
                        topHits("hits")
                    )
                )
                .get();
            Terms terms = response.getAggregations().get("terms");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                TopHits topHits = bucket.getAggregations().get("hits");
                for (SearchHit hit : topHits.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(4.0f));
                }
            }
        }

        {
            SearchResponse response = client()
                .prepareSearch("idx")
                .addRescorer(
                        new QueryRescorerBuilder(new MatchAllQueryBuilder().boost(3.0f))
                )
                .addAggregation(terms("terms")
                    .field(TERMS_AGGS_FIELD)
                    .subAggregation(
                        topHits("hits").sort(SortBuilders.scoreSort())
                    )
                )
                .get();
            Terms terms = response.getAggregations().get("terms");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                TopHits topHits = bucket.getAggregations().get("hits");
                for (SearchHit hit : topHits.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(4.0f));
                }
            }
        }

        // Rescore should not be applied if the sort order is not relevancy
        {
            SearchResponse response = client()
                .prepareSearch("idx")
                .addRescorer(
                    new QueryRescorerBuilder(new MatchAllQueryBuilder().boost(3.0f))
                )
                .addAggregation(terms("terms")
                    .field(TERMS_AGGS_FIELD)
                    .subAggregation(
                        topHits("hits").sort(SortBuilders.fieldSort("_type"))
                    )
                )
                .get();
            Terms terms = response.getAggregations().get("terms");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                TopHits topHits = bucket.getAggregations().get("hits");
                for (SearchHit hit : topHits.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(Float.NaN));
                }
            }
        }

        {
            SearchResponse response = client()
                .prepareSearch("idx")
                .addRescorer(
                        new QueryRescorerBuilder(new MatchAllQueryBuilder().boost(3.0f))
                )
                .addAggregation(terms("terms")
                    .field(TERMS_AGGS_FIELD)
                    .subAggregation(
                        topHits("hits").sort(SortBuilders.scoreSort()).sort(SortBuilders.fieldSort("_type"))
                    )
                )
                .get();
            Terms terms = response.getAggregations().get("terms");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                TopHits topHits = bucket.getAggregations().get("hits");
                for (SearchHit hit : topHits.getHits().getHits()) {
                    assertThat(hit.getScore(), equalTo(Float.NaN));
                }
            }
        }
    }
}
