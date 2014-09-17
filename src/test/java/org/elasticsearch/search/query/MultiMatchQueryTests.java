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
package org.elasticsearch.search.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

public class MultiMatchQueryTests extends ElasticsearchIntegrationTest {

    @Before
    public void init() throws Exception {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.perfect_match.type", "custom")
                .put("index.analysis.analyzer.perfect_match.tokenizer", "keyword")
                .put("index.analysis.analyzer.perfect_match.filter", "lowercase")
                .put("index.analysis.analyzer.category.type", "custom")
                .put("index.analysis.analyzer.category.tokenizer", "whitespace")
                .put("index.analysis.analyzer.category.filter", "lowercase")
        );
        assertAcked(builder.addMapping("test", createMapping()));
        ensureGreen();
        int numDocs = scaledRandomIntBetween(50, 100);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test", "test", "theone").setSource(
                "full_name", "Captain America",
                "first_name", "Captain",
                "last_name", "America",
                "category", "marvel hero",
                "skill", 15,
                "int-field", 25));
        builders.add(client().prepareIndex("test", "test", "theother").setSource(
                "full_name", "marvel hero",
                "first_name", "marvel",
                "last_name", "hero",
                "category", "bogus",
                "skill", 5));

        builders.add(client().prepareIndex("test", "test", "ultimate1").setSource(
                "full_name", "Alpha the Ultimate Mutant",
                "first_name", "Alpha the",
                "last_name", "Ultimate Mutant",
                "category", "marvel hero",
                "skill", 1));
        builders.add(client().prepareIndex("test", "test", "ultimate2").setSource(
                "full_name", "Man the Ultimate Ninja",
                "first_name", "Man the Ultimate",
                "last_name", "Ninja",
                "category", "marvel hero",
                "skill", 3));

        builders.add(client().prepareIndex("test", "test", "anotherhero").setSource(
                "full_name", "ultimate",
                "first_name", "wolferine",
                "last_name", "",
                "category", "marvel hero",
                "skill", 1));
        List<String> firstNames = new ArrayList<>();
        fill(firstNames, "Captain", between(15, 25));
        fill(firstNames, "Ultimate", between(5, 10));
        fillRandom(firstNames, between(3, 7));
        List<String> lastNames = new ArrayList<>();
        fill(lastNames, "Captain", between(3, 7));
        fillRandom(lastNames, between(30, 40));
        for (int i = 0; i < numDocs; i++) {
            String first = RandomPicks.randomFrom(getRandom(), firstNames);
            String last = randomPickExcept(lastNames, first);
            builders.add(client().prepareIndex("test", "test", "" + i).setSource(
                    "full_name", first + " " + last,
                    "first_name", first,
                    "last_name", last,
                    "category", randomBoolean() ? "marvel hero" : "bogus",
                    "skill", between(1, 3)));
        }
        indexRandom(true, false, builders);
    }

    private XContentBuilder createMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("test")
                .startObject("properties")
                .startObject("full_name")
                .field("type", "string")
                .field("copy_to", "full_name_phrase")
                .field("analyzer", "perfect_match")
                .endObject()
                .startObject("category")
                .field("type", "string")
                .field("analyzer", "category")
                .field("index_option", "docs")
                .endObject()
                .startObject("first_name")
                .field("type", "string")
                .field("omit_norms", "true")
                .field("copy_to", "first_name_phrase")
                .field("index_option", "docs")
                .endObject()
                .startObject("last_name")
                .field("type", "string")
                .field("omit_norms", "true")
                .field("copy_to", "last_name_phrase")
                .field("index_option", "docs")
                .endObject()
                .endObject()
                .endObject().endObject();
    }

    @Test
    public void testDefaults() throws ExecutionException, InterruptedException {
        MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR))).get();
        Set<String> topNIds = Sets.newHashSet("theone", "theother");
        for (int i = 0; i < searchResponse.getHits().hits().length; i++) {
            topNIds.remove(searchResponse.getHits().getAt(i).getId());
            // very likely that we hit a random doc that has the same score so orders are random since
            // the doc id is the tie-breaker
        }
        assertThat(topNIds, empty());
        assertThat(searchResponse.getHits().hits()[0].getScore(), equalTo(searchResponse.getHits().hits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).useDisMax(false).type(type))).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).type(type))).get();
        assertFirstHit(searchResponse, hasId("theother"));


        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).type(type))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).type(type))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));
    }

    @Test
    public void testPhraseType() {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("Man the Ultimate", "full_name_phrase", "first_name_phrase", "last_name_phrase", "category_phrase")
                        .operator(MatchQueryBuilder.Operator.OR).type(MatchQueryBuilder.Type.PHRASE))).get();
        assertFirstHit(searchResponse, hasId("ultimate2"));
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("Captain", "full_name_phrase", "first_name_phrase", "last_name_phrase", "category_phrase")
                        .operator(MatchQueryBuilder.Operator.OR).type(MatchQueryBuilder.Type.PHRASE))).get();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("the Ul", "full_name_phrase", "first_name_phrase", "last_name_phrase", "category_phrase")
                        .operator(MatchQueryBuilder.Operator.OR).type(MatchQueryBuilder.Type.PHRASE_PREFIX))).get();
        assertSearchHits(searchResponse, "ultimate2", "ultimate1");
        assertHitCount(searchResponse, 2l);
    }

    @Test
    public void testSingleField() throws NoSuchFieldException, IllegalAccessException {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "skill"))).get();
        assertNoFailures(searchResponse);
        assertFirstHit(searchResponse, hasId("theone"));
        String[] fields = {"full_name", "first_name", "last_name", "last_name_phrase", "first_name_phrase", "category_phrase", "category"};

        String[] query = {"marvel","hero", "captain",  "america", "15", "17", "1", "5", "ultimate", "Man",
                "marvel", "wolferine", "ninja"};

        // check if it's equivalent to a match query.
        int numIters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numIters; i++) {
            String field = RandomPicks.randomFrom(getRandom(), fields);
            int numTerms = randomIntBetween(1, query.length);
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < numTerms; j++) {
                builder.append(RandomPicks.randomFrom(getRandom(), query)).append(" ");
            }
            MultiMatchQueryBuilder multiMatchQueryBuilder = randomizeType(multiMatchQuery(builder.toString(), field));
            SearchResponse multiMatchResp = client().prepareSearch("test")
                    // _uid sort field is a tie, in case hits have the same score,
                    // the hits will be sorted the same consistently
                    .addSort("_score", SortOrder.DESC)
                    .addSort("_uid", SortOrder.ASC)
                    .setQuery(multiMatchQueryBuilder).get();
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, builder.toString());
            if (getType(multiMatchQueryBuilder) != null) {
                matchQueryBuilder.type(MatchQueryBuilder.Type.valueOf(getType(multiMatchQueryBuilder).matchQueryType().toString()));
            }
            SearchResponse matchResp = client().prepareSearch("test")
                    // _uid tie sort
                    .addSort("_score", SortOrder.DESC)
                    .addSort("_uid", SortOrder.ASC)
                    .setQuery(matchQueryBuilder).get();
            assertThat("field: " + field + " query: " + builder.toString(), multiMatchResp.getHits().getTotalHits(), equalTo(matchResp.getHits().getTotalHits()));
            SearchHits hits = multiMatchResp.getHits();
            for (int j = 0; j < hits.hits().length; j++) {
                assertThat(hits.getHits()[j].score(), equalTo(matchResp.getHits().getHits()[j].score()));
                assertThat(hits.getHits()[j].getId(), equalTo(matchResp.getHits().getHits()[j].getId()));
            }
        }

    }

    @Test
    public void testCutoffFreq() throws ExecutionException, InterruptedException {
        final long numDocs = client().prepareCount("test")
                .setQuery(matchAllQuery()).get().getCount();
        MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
        Float cutoffFrequency = randomBoolean() ? Math.min(1, numDocs * 1.f / between(10, 20)) : 1.f / between(10, 20);
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).cutoffFrequency(cutoffFrequency))).get();
        Set<String> topNIds = Sets.newHashSet("theone", "theother");
        for (int i = 0; i < searchResponse.getHits().hits().length; i++) {
            topNIds.remove(searchResponse.getHits().getAt(i).getId());
            // very likely that we hit a random doc that has the same score so orders are random since
            // the doc id is the tie-breaker
        }
        assertThat(topNIds, empty());
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThanOrEqualTo(searchResponse.getHits().hits()[1].getScore()));

        cutoffFrequency = randomBoolean() ? Math.min(1, numDocs * 1.f / between(10, 20)) : 1.f / between(10, 20);
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).useDisMax(false).cutoffFrequency(cutoffFrequency).type(type))).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));
        long size = searchResponse.getHits().getTotalHits();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).useDisMax(false).type(type))).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat("common terms expected to be a way smaller result set", size, lessThan(searchResponse.getHits().getTotalHits()));

        cutoffFrequency = randomBoolean() ? Math.min(1, numDocs * 1.f / between(10, 20)) : 1.f / between(10, 20);
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).cutoffFrequency(cutoffFrequency).type(type))).get();
        assertFirstHit(searchResponse, hasId("theother"));


        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).cutoffFrequency(cutoffFrequency).type(type))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).cutoffFrequency(cutoffFrequency).type(type))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).cutoffFrequency(cutoffFrequency)
                        .analyzer("category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theother"));
    }


    @Test
    public void testEquivalence() {

        final int numDocs = (int) client().prepareCount("test")
                .setQuery(matchAllQuery()).get().getCount();
        int numIters = scaledRandomIntBetween(5, 10);
        for (int i = 0; i < numIters; i++) {
            {
                MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
                MultiMatchQueryBuilder multiMatchQueryBuilder = randomBoolean() ? multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category") :
                        multiMatchQuery("marvel hero captain america", "*_name", randomBoolean() ? "category" : "categ*");
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(randomizeType(multiMatchQueryBuilder
                                .operator(MatchQueryBuilder.Operator.OR).type(type))).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(disMaxQuery().
                                add(matchQuery("full_name", "marvel hero captain america"))
                                .add(matchQuery("first_name", "marvel hero captain america"))
                                .add(matchQuery("last_name", "marvel hero captain america"))
                                .add(matchQuery("category", "marvel hero captain america"))
                        ).get();
                assertEquivalent("marvel hero captain america", left, right);
            }

            {
                MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
                String minShouldMatch = randomBoolean() ? null : "" + between(0, 1);
                MatchQueryBuilder.Operator op = randomBoolean() ? MatchQueryBuilder.Operator.AND : MatchQueryBuilder.Operator.OR;
                MultiMatchQueryBuilder multiMatchQueryBuilder = randomBoolean() ? multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category") :
                        multiMatchQuery("captain america", "*_name", randomBoolean() ? "category" : "categ*");
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(randomizeType(multiMatchQueryBuilder
                                .operator(op).useDisMax(false).minimumShouldMatch(minShouldMatch).type(type))).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(boolQuery().minimumShouldMatch(minShouldMatch)
                                .should(randomBoolean() ? termQuery("full_name", "captain america") : matchQuery("full_name", "captain america").operator(op))
                                .should(matchQuery("first_name", "captain america").operator(op))
                                .should(matchQuery("last_name", "captain america").operator(op))
                                .should(matchQuery("category", "captain america").operator(op))
                        ).get();
                assertEquivalent("captain america", left, right);
            }

            {
                String minShouldMatch = randomBoolean() ? null : "" + between(0, 1);
                MatchQueryBuilder.Operator op = randomBoolean() ? MatchQueryBuilder.Operator.AND : MatchQueryBuilder.Operator.OR;
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(randomizeType(multiMatchQuery("capta", "full_name", "first_name", "last_name", "category")
                                .type(MatchQueryBuilder.Type.PHRASE_PREFIX).useDisMax(false).minimumShouldMatch(minShouldMatch))).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(boolQuery().minimumShouldMatch(minShouldMatch)
                                .should(matchPhrasePrefixQuery("full_name", "capta"))
                                .should(matchPhrasePrefixQuery("first_name", "capta").operator(op))
                                .should(matchPhrasePrefixQuery("last_name", "capta").operator(op))
                                .should(matchPhrasePrefixQuery("category", "capta").operator(op))
                        ).get();
                assertEquivalent("capta", left, right);
            }
            {
                String minShouldMatch = randomBoolean() ? null : "" + between(0, 1);
                MatchQueryBuilder.Operator op = randomBoolean() ? MatchQueryBuilder.Operator.AND : MatchQueryBuilder.Operator.OR;
                SearchResponse left;
                if (randomBoolean()) {
                    left = client().prepareSearch("test").setSize(numDocs)
                            .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                            .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                                    .type(MatchQueryBuilder.Type.PHRASE).useDisMax(false).minimumShouldMatch(minShouldMatch))).get();
                } else {
                    left = client().prepareSearch("test").setSize(numDocs)
                            .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                            .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                                    .type(MatchQueryBuilder.Type.PHRASE).tieBreaker(1.0f).minimumShouldMatch(minShouldMatch))).get();
                }
                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("_uid"))
                        .setQuery(boolQuery().minimumShouldMatch(minShouldMatch)
                                .should(matchPhraseQuery("full_name", "captain america"))
                                .should(matchPhraseQuery("first_name", "captain america").operator(op))
                                .should(matchPhraseQuery("last_name", "captain america").operator(op))
                                .should(matchPhraseQuery("category", "captain america").operator(op))
                        ).get();
                assertEquivalent("captain america", left, right);
            }
        }
    }

    @Test
    public void testCrossFieldMode() throws ExecutionException, InterruptedException {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(MatchQueryBuilder.Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(MatchQueryBuilder.Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theone"));
        assertSecondHit(searchResponse, hasId("theother"));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(MatchQueryBuilder.Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theother"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america 15", "full_name", "first_name", "last_name", "category", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america 15", "first_name", "last_name", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("25 15", "int-field", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .cutoffFrequency(0.1f)
                        .analyzer("category")
                        .operator(MatchQueryBuilder.Operator.OR))).get();
        assertFirstHit(searchResponse, anyOf(hasId("theother"), hasId("theone")));
        long numResults = searchResponse.getHits().totalHits();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .operator(MatchQueryBuilder.Operator.OR))).get();
        assertThat(numResults, lessThan(searchResponse.getHits().getTotalHits()));
        assertFirstHit(searchResponse, hasId("theone"));


        // test group based on analyzer -- all fields are grouped into a cross field search
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));
        // counter example
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(randomBoolean() ? MultiMatchQueryBuilder.Type.CROSS_FIELDS : null)
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertHitCount(searchResponse, 0l);

        // counter example
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(randomBoolean() ? MultiMatchQueryBuilder.Type.CROSS_FIELDS : null)
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertHitCount(searchResponse, 0l);

        // test if boosts work
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("the ultimate", "full_name", "first_name", "last_name^2", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertFirstHit(searchResponse, hasId("ultimate1"));   // has ultimate in the last_name and that is boosted
        assertSecondHit(searchResponse, hasId("ultimate2"));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));

        // since we try to treat the matching fields as one field scores are very similar but we have a small bias towards the
        // more frequent field that acts as a tie-breaker internally
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("the ultimate", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(MatchQueryBuilder.Operator.AND))).get();
        assertFirstHit(searchResponse, hasId("ultimate2"));
        assertSecondHit(searchResponse, hasId("ultimate1"));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));
    }

    private static final void assertEquivalent(String query, SearchResponse left, SearchResponse right) {
        assertNoFailures(left);
        assertNoFailures(right);
        SearchHits leftHits = left.getHits();
        SearchHits rightHits = right.getHits();
        assertThat(leftHits.getTotalHits(), equalTo(rightHits.getTotalHits()));
        assertThat(leftHits.getHits().length, equalTo(rightHits.getHits().length));
        SearchHit[] hits = leftHits.getHits();
        SearchHit[] rHits = rightHits.getHits();
        for (int i = 0; i < hits.length; i++) {
            assertThat("query: " + query + " hit: " + i, (double) hits[i].getScore(), closeTo(rHits[i].getScore(), 0.00001d));
        }
        for (int i = 0; i < hits.length; i++) {
            if (hits[i].getScore() == hits[hits.length - 1].getScore()) {
                return; // we need to cut off here since this is the tail of the queue and we might not have fetched enough docs
            }
            assertThat("query: " + query, hits[i].getId(), equalTo(rHits[i].getId()));
        }
    }


    public static List<String> fill(List<String> list, String value, int times) {
        for (int i = 0; i < times; i++) {
            list.add(value);
        }
        return list;
    }

    public List<String> fillRandom(List<String> list, int times) {
        for (int i = 0; i < times; i++) {
            list.add(randomRealisticUnicodeOfCodepointLengthBetween(1, 5));
        }
        return list;
    }

    public <T> T randomPickExcept(List<T> fromList, T butNot) {
        while (true) {
            T t = RandomPicks.randomFrom(getRandom(), fromList);
            if (t.equals(butNot)) {
                continue;
            }
            return t;
        }
    }

    public MultiMatchQueryBuilder randomizeType(MultiMatchQueryBuilder builder) {
        try {
            MultiMatchQueryBuilder.Type type = getType(builder);
            if (type == null && randomBoolean()) {
                return builder;
            }
            if (type == null) {
                type = MultiMatchQueryBuilder.Type.BEST_FIELDS;
            }
            if (randomBoolean()) {
                builder.type(type);
            } else {
                Object oType = type;
                switch (type) {
                    case BEST_FIELDS:
                        if (randomBoolean()) {
                            oType = MatchQueryBuilder.Type.BOOLEAN;
                        }
                        break;
                    case MOST_FIELDS:
                        if (randomBoolean()) {
                            oType = MatchQueryBuilder.Type.BOOLEAN;
                        }
                        break;
                    case CROSS_FIELDS:
                        break;
                    case PHRASE:
                        if (randomBoolean()) {
                            oType = MatchQueryBuilder.Type.PHRASE;
                        }
                        break;
                    case PHRASE_PREFIX:
                        if (randomBoolean()) {
                            oType = MatchQueryBuilder.Type.PHRASE_PREFIX;
                        }
                        break;
                }
                builder.type(oType);
            }
            return builder;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private MultiMatchQueryBuilder.Type getType(MultiMatchQueryBuilder builder) throws NoSuchFieldException, IllegalAccessException {
        Field field = MultiMatchQueryBuilder.class.getDeclaredField("type");
        field.setAccessible(true);
        return (MultiMatchQueryBuilder.Type) field.get(builder);
    }
}
