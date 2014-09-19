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
package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest()
public class TopHitsTests extends ElasticsearchIntegrationTest {

    private static final String TERMS_AGGS_FIELD = "terms";
    private static final String SORT_FIELD = "sort";

    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("empty");
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

        builders.add(client().prepareIndex("idx", "field-collapsing", "1").setSource(jsonBuilder()
                .startObject()
                .field("group", "a")
                .field("text", "term x y z b")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "2").setSource(jsonBuilder()
                .startObject()
                .field("group", "a")
                .field("text", "term x y z n rare")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "3").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "x y z term")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "4").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "x y term")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "5").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "x term")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "6").setSource(jsonBuilder()
                .startObject()
                .field("group", "b")
                .field("text", "term rare")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "7").setSource(jsonBuilder()
                .startObject()
                .field("group", "c")
                .field("text", "x y z term")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "8").setSource(jsonBuilder()
                .startObject()
                .field("group", "c")
                .field("text", "x y term b")
                .endObject()));
        builders.add(client().prepareIndex("idx", "field-collapsing", "9").setSource(jsonBuilder()
                .startObject()
                .field("group", "c")
                .field("text", "rare x term")
                .endObject()));

        indexRandom(true, builders);
        ensureSearchable();
    }

    private String key(Terms.Bucket bucket) {
        return randomBoolean() ? bucket.getKey() : bucket.getKeyAsText().string();
    }

    @Test
    public void testBasics() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(TERMS_AGGS_FIELD)
                        .subAggregation(
                                topHits("hits").addSort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
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
            assertThat(bucket.getDocCount(), equalTo(10l));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.totalHits(), equalTo(10l));
            assertThat(hits.getHits().length, equalTo(3));
            higestSortValue += 10;
            assertThat((Long) hits.getAt(0).sortValues()[0], equalTo(higestSortValue));
            assertThat((Long) hits.getAt(1).sortValues()[0], equalTo(higestSortValue - 1));
            assertThat((Long) hits.getAt(2).sortValues()[0], equalTo(higestSortValue - 2));

            assertThat(hits.getAt(0).sourceAsMap().size(), equalTo(4));
        }
    }

    @Test
    public void testPagination() throws Exception {
        int size = randomIntBetween(1, 10);
        int from = randomIntBetween(0, 10);
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .subAggregation(
                                        topHits("hits").addSort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))
                                                .setFrom(from)
                                                .setSize(size)
                                )
                )
                .get();
        assertSearchResponse(response);

        SearchResponse control = client().prepareSearch("idx")
                .setTypes("type")
                .setFrom(from)
                .setSize(size)
                .setPostFilter(FilterBuilders.termFilter(TERMS_AGGS_FIELD, "val0"))
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
        assertThat(bucket.getDocCount(), equalTo(10l));
        TopHits topHits = bucket.getAggregations().get("hits");
        SearchHits hits = topHits.getHits();
        assertThat(hits.totalHits(), equalTo(controlHits.totalHits()));
        assertThat(hits.getHits().length, equalTo(controlHits.getHits().length));
        for (int i = 0; i < hits.getHits().length; i++) {
            logger.info(i + ": top_hits: [" + hits.getAt(i).id() + "][" + hits.getAt(i).sortValues()[0] + "] control: [" + controlHits.getAt(i).id() + "][" + controlHits.getAt(i).sortValues()[0] + "]");
            assertThat(hits.getAt(i).id(), equalTo(controlHits.getAt(i).id()));
            assertThat(hits.getAt(i).sortValues()[0], equalTo(controlHits.getAt(i).sortValues()[0]));
        }
    }

    @Test
    public void testSortByBucket() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .order(Terms.Order.aggregation("max_sort", false))
                                .subAggregation(
                                        topHits("hits").addSort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC)).setTrackScores(true)
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
            assertThat(bucket.getDocCount(), equalTo(10l));
            TopHits topHits = bucket.getAggregations().get("hits");
            SearchHits hits = topHits.getHits();
            assertThat(hits.totalHits(), equalTo(10l));
            assertThat(hits.getHits().length, equalTo(3));
            assertThat((Long) hits.getAt(0).sortValues()[0], equalTo(higestSortValue));
            assertThat((Long) hits.getAt(1).sortValues()[0], equalTo(higestSortValue - 1));
            assertThat((Long) hits.getAt(2).sortValues()[0], equalTo(higestSortValue - 2));
            Max max = bucket.getAggregations().get("max_sort");
            assertThat(max.getValue(), equalTo(((Long) higestSortValue).doubleValue()));
            higestSortValue -= 10;
        }
    }

    @Test
    public void testFieldCollapsing() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("field-collapsing")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(matchQuery("text", "term rare"))
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field("group")
                                .order(Terms.Order.aggregation("max_score", false))
                                .subAggregation(
                                        topHits("hits").setSize(1)
                                )
                                .subAggregation(
                                        max("max_score").script("_score.doubleValue()")
                                )
                )
                .get();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));

        Iterator<Terms.Bucket> bucketIterator = terms.getBuckets().iterator();
        Terms.Bucket bucket = bucketIterator.next();
        assertThat(key(bucket), equalTo("b"));
        TopHits topHits = bucket.getAggregations().get("hits");
        SearchHits hits = topHits.getHits();
        assertThat(hits.totalHits(), equalTo(4l));
        assertThat(hits.getHits().length, equalTo(1));
        assertThat(hits.getAt(0).id(), equalTo("6"));

        bucket = bucketIterator.next();
        assertThat(key(bucket), equalTo("c"));
        topHits = bucket.getAggregations().get("hits");
        hits = topHits.getHits();
        assertThat(hits.totalHits(), equalTo(3l));
        assertThat(hits.getHits().length, equalTo(1));
        assertThat(hits.getAt(0).id(), equalTo("9"));

        bucket = bucketIterator.next();
        assertThat(key(bucket), equalTo("a"));
        topHits = bucket.getAggregations().get("hits");
        hits = topHits.getHits();
        assertThat(hits.totalHits(), equalTo(2l));
        assertThat(hits.getHits().length, equalTo(1));
        assertThat(hits.getAt(0).id(), equalTo("2"));
    }

    @Test
    public void testFetchFeatures() {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(matchQuery("text", "text").queryName("test"))
                .addAggregation(terms("terms")
                                .executionHint(randomExecutionHint())
                                .field(TERMS_AGGS_FIELD)
                                .subAggregation(
                                        topHits("hits").setSize(1)
                                            .addHighlightedField("text")
                                            .setExplain(true)
                                            .addFieldDataField("field1")
                                            .addScriptField("script", "doc['field1'].value")
                                            .setFetchSource("text", null)
                                            .setVersion(true)
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
            assertThat(hits.totalHits(), equalTo(10l));
            assertThat(hits.getHits().length, equalTo(1));

            SearchHit hit = hits.getAt(0);
            HighlightField highlightField = hit.getHighlightFields().get("text");
            assertThat(highlightField.getFragments().length, equalTo(1));
            assertThat(highlightField.getFragments()[0].string(), equalTo("some <em>text</em> to entertain"));

            Explanation explanation = hit.explanation();
            assertThat(explanation.toString(), containsString("text:text"));

            long version = hit.version();
            assertThat(version, equalTo(1l));

            assertThat(hit.matchedQueries()[0], equalTo("test"));

            SearchHitField field = hit.field("field1");
            assertThat(field.getValue().toString(), equalTo("5"));

            field = hit.field("script");
            assertThat(field.getValue().toString(), equalTo("5"));

            assertThat(hit.sourceAsMap().size(), equalTo(1));
            assertThat(hit.sourceAsMap().get("text").toString(), equalTo("some text to entertain"));
        }
    }

    @Test
    public void testInvalidSortField() throws Exception {
        try {
            client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                                    .executionHint(randomExecutionHint())
                                    .field(TERMS_AGGS_FIELD)
                                    .subAggregation(
                                            topHits("hits").addSort(SortBuilders.fieldSort("xyz").order(SortOrder.DESC))
                                    )
                    ).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("No mapping found for [xyz] in order to sort on"));
        }
    }

    @Test
    public void testFailWithSubAgg() throws Exception {
        String source = "{\n" +
                "  \"aggs\": {\n" +
                "    \"top-tags\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"tags\"\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"top_tags_hits\": {\n" +
                "          \"top_hits\": {},\n" +
                "          \"aggs\": {\n" +
                "            \"max\": {\n" +
                "              \"max\": {\n" +
                "                \"field\": \"age\"\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        try {
            client().prepareSearch("idx").setTypes("type")
                    .setSource(source)
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("Aggregator [top_tags_hits] of type [top_hits] cannot accept sub-aggregations"));
        }
    }
    
    @Test
    public void testFailDeferredOnlyWhenScorerIsUsed() throws Exception {
        // No track_scores or score based sort defined in top_hits agg, so don't fail:
        SearchResponse response = client().prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(TERMS_AGGS_FIELD)
                                .collectMode(SubAggCollectionMode.BREADTH_FIRST)
                                .subAggregation(topHits("hits").addSort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC))))
                .get();
        assertSearchResponse(response);

        // Score based, so fail with deferred aggs:
        try {
            client().prepareSearch("idx")
                    .setTypes("type")
                    .addAggregation(
                            terms("terms").executionHint(randomExecutionHint()).field(TERMS_AGGS_FIELD)
                                    .collectMode(SubAggCollectionMode.BREADTH_FIRST)
                                    .subAggregation(topHits("hits")))
                    .get();
            fail();
        } catch (Exception e) {
            // It is considered a parse failure if the search request asks for top_hits
            // under an aggregation with collect_mode set to breadth_first as this would
            // require the buffering of scores alongside each document ID and that is a
            // a RAM cost we are not willing to pay 
            assertThat(e.getMessage(), containsString("ElasticsearchParseException"));
        }
    }

    @Test
    public void testEmptyIndex() throws Exception {
        SearchResponse response = client().prepareSearch("empty").setTypes("type")
                .addAggregation(topHits("hits"))
                .get();
        assertSearchResponse(response);

        TopHits hits = response.getAggregations().get("hits");
        assertThat(hits, notNullValue());
        assertThat(hits.getName(), equalTo("hits"));
        assertThat(hits.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testTrackScores() throws Exception {
        boolean[] trackScores = new boolean[]{true, false};
        for (boolean trackScore : trackScores) {
            logger.info("Track score=" + trackScore);
            SearchResponse response = client().prepareSearch("idx").setTypes("field-collapsing")
                    .setQuery(matchQuery("text", "term rare"))
                    .addAggregation(terms("terms")
                                    .field("group")
                                    .subAggregation(
                                            topHits("hits")
                                                    .setTrackScores(trackScore)
                                                    .setSize(1)
                                                    .addSort("_id", SortOrder.DESC)
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
            assertThat(hits.getAt(0).score(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));

            bucket = terms.getBucketByKey("b");
            assertThat(key(bucket), equalTo("b"));
            topHits = bucket.getAggregations().get("hits");
            hits = topHits.getHits();
            assertThat(hits.getMaxScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
            assertThat(hits.getAt(0).score(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));

            bucket = terms.getBucketByKey("c");
            assertThat(key(bucket), equalTo("c"));
            topHits = bucket.getAggregations().get("hits");
            hits = topHits.getHits();
            assertThat(hits.getMaxScore(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
            assertThat(hits.getAt(0).score(), trackScore ? not(equalTo(Float.NaN)) : equalTo(Float.NaN));
        }
    }

}
