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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockKeywordPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.disMaxQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhrasePrefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MultiMatchQueryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MockKeywordPlugin.class);
    }

    @Before
    public void init() throws Exception {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.perfect_match.type", "custom")
                .put("index.analysis.analyzer.perfect_match.tokenizer", "keyword")
                .put("index.analysis.analyzer.perfect_match.filter", "lowercase")
                .put("index.analysis.analyzer.category.type", "custom")
                .put("index.analysis.analyzer.category.tokenizer", "standard")
                .put("index.analysis.analyzer.category.filter", "lowercase")
        );
        assertAcked(builder.setMapping(createMapping()));
        ensureGreen();
        int numDocs = scaledRandomIntBetween(50, 100);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test").setId("theone").setSource(
                "id", "theone",
                "full_name", "Captain America",
                "first_name", "Captain",
                "last_name", "America",
                "category", "marvel hero",
                "skill", 15,
                "int-field", 25));
        builders.add(client().prepareIndex("test").setId("theother").setSource(
                "id", "theother",
                "full_name", "marvel hero",
                "first_name", "marvel",
                "last_name", "hero",
                "category", "bogus",
                "skill", 5));

        builders.add(client().prepareIndex("test").setId("ultimate1").setSource(
                "id", "ultimate1",
                "full_name", "Alpha the Ultimate Mutant",
                "first_name", "Alpha the",
                "last_name", "Ultimate Mutant",
                "category", "marvel hero",
                "skill", 1));
        builders.add(client().prepareIndex("test").setId("ultimate2").setSource(
                "full_name", "Man the Ultimate Ninja",
                "first_name", "Man the Ultimate",
                "last_name", "Ninja",
                "category", "marvel hero",
                "skill", 3));

        builders.add(client().prepareIndex("test").setId("anotherhero").setSource(
                "id", "anotherhero",
                "full_name", "ultimate",
                "first_name", "wolferine",
                "last_name", "",
                "category", "marvel hero",
                "skill", 1));

        builders.add(client().prepareIndex("test").setId("nowHero").setSource(
                "id", "nowHero",
                "full_name", "now sort of",
                "first_name", "now",
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
            String first = RandomPicks.randomFrom(random(), firstNames);
            String last = randomPickExcept(lastNames, first);
            builders.add(client().prepareIndex("test").setId("" + i).setSource(
                    "id", i,
                    "full_name", first + " " + last,
                    "first_name", first,
                    "last_name", last,
                    "category", randomBoolean() ? "marvel hero" : "bogus",
                    "skill", between(1, 3)));
        }
        indexRandom(true, false, builders);
    }

    private XContentBuilder createMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties")
                .startObject("id")
                .field("type", "keyword")
                .endObject()
                .startObject("full_name")
                .field("type", "text")
                .field("copy_to", "full_name_phrase")
                .field("analyzer", "perfect_match")
                .endObject()
                .startObject("category")
                .field("type", "text")
                .field("analyzer", "category")
                .endObject()
                .startObject("first_name")
                .field("type", "text")
                .field("norms", false)
                .field("copy_to", "first_name_phrase")
                .endObject()
                .startObject("last_name")
                .field("type", "text")
                .field("norms", false)
                .field("copy_to", "last_name_phrase")
                .endObject()
                .startObject("date")
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject().endObject();
    }

    public void testDefaults() throws ExecutionException, InterruptedException {
        MatchQuery.Type type = MatchQuery.Type.BOOLEAN;
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(Operator.OR))).get();
        Set<String> topNIds = Sets.newHashSet("theone", "theother");
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            topNIds.remove(searchResponse.getHits().getAt(i).getId());
            // very likely that we hit a random doc that has the same score so orders are random since
            // the doc id is the tie-breaker
        }
        assertThat(topNIds, empty());
        assertThat(searchResponse.getHits().getHits()[0].getScore(), greaterThan(searchResponse.getHits().getHits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(Operator.OR).type(type))).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat(searchResponse.getHits().getHits()[0].getScore(), greaterThan(searchResponse.getHits().getHits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .operator(Operator.OR).type(type))).get();
        assertFirstHit(searchResponse, hasId("theother"));


        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(Operator.AND).type(type))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(Operator.AND).type(type))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));
    }

    public void testPhraseType() {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("Man the Ultimate", "full_name_phrase", "first_name_phrase", "last_name_phrase",
                        "category_phrase")
                        .operator(Operator.OR).type(MatchQuery.Type.PHRASE))).get();
        assertFirstHit(searchResponse, hasId("ultimate2"));
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("Captain", "full_name_phrase", "first_name_phrase", "last_name_phrase",
                        "category_phrase")
                        .operator(Operator.OR).type(MatchQuery.Type.PHRASE))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(1L));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("the Ul", "full_name_phrase", "first_name_phrase", "last_name_phrase",
                        "category_phrase")
                        .operator(Operator.OR).type(MatchQuery.Type.PHRASE_PREFIX))).get();
        assertSearchHits(searchResponse, "ultimate2", "ultimate1");
        assertHitCount(searchResponse, 2L);
    }

    public void testSingleField() throws NoSuchFieldException, IllegalAccessException {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "skill"))).get();
        assertNoFailures(searchResponse);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "skill", "int-field")).analyzer("category")).get();
        assertNoFailures(searchResponse);
        assertFirstHit(searchResponse, hasId("theone"));

        String[] fields = { "full_name", "first_name", "last_name", "last_name_phrase", "first_name_phrase", "category_phrase", "category",
                "missing_field", "missing_fields*" };

        String[] query = {"marvel","hero", "captain",  "america", "15", "17", "1", "5", "ultimate", "Man",
                "marvel", "wolferine", "ninja"};

        // check if it's equivalent to a match query.
        int numIters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numIters; i++) {
            String field = RandomPicks.randomFrom(random(), fields);
            int numTerms = randomIntBetween(1, query.length);
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < numTerms; j++) {
                builder.append(RandomPicks.randomFrom(random(), query)).append(" ");
            }
            MultiMatchQueryBuilder multiMatchQueryBuilder = randomizeType(multiMatchQuery(builder.toString(), field));
            SearchResponse multiMatchResp = client().prepareSearch("test")
                    // id sort field is a tie, in case hits have the same score,
                    // the hits will be sorted the same consistently
                    .addSort("_score", SortOrder.DESC)
                    .addSort("id", SortOrder.ASC)
                    .setQuery(multiMatchQueryBuilder).get();
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, builder.toString());

            SearchResponse matchResp = client().prepareSearch("test")
                    // id tie sort
                    .addSort("_score", SortOrder.DESC)
                    .addSort("id", SortOrder.ASC)
                    .setQuery(matchQueryBuilder).get();
            assertThat("field: " + field + " query: " + builder.toString(), multiMatchResp.getHits().getTotalHits().value,
                    equalTo(matchResp.getHits().getTotalHits().value));
            SearchHits hits = multiMatchResp.getHits();
            if (field.startsWith("missing")) {
                assertEquals(0, hits.getHits().length);
            }
            for (int j = 0; j < hits.getHits().length; j++) {
                assertThat(hits.getHits()[j].getScore(), equalTo(matchResp.getHits().getHits()[j].getScore()));
                assertThat(hits.getHits()[j].getId(), equalTo(matchResp.getHits().getHits()[j].getId()));
            }
        }

    }

    public void testEquivalence() {

        final int numDocs = (int) client().prepareSearch("test").setSize(0)
                .setQuery(matchAllQuery()).get().getHits().getTotalHits().value;
        int numIters = scaledRandomIntBetween(5, 10);
        for (int i = 0; i < numIters; i++) {
            {
                MatchQuery.Type type = MatchQuery.Type.BOOLEAN;
                MultiMatchQueryBuilder multiMatchQueryBuilder = randomBoolean() ?
                        multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category") :
                        multiMatchQuery("marvel hero captain america", "*_name", randomBoolean() ? "category" : "categ*");
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(randomizeType(multiMatchQueryBuilder
                                .operator(Operator.OR).type(type))).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(disMaxQuery().
                                add(matchQuery("full_name", "marvel hero captain america"))
                                .add(matchQuery("first_name", "marvel hero captain america"))
                                .add(matchQuery("last_name", "marvel hero captain america"))
                                .add(matchQuery("category", "marvel hero captain america"))
                        ).get();
                assertEquivalent("marvel hero captain america", left, right);
            }

            {
                MatchQuery.Type type = MatchQuery.Type.BOOLEAN;
                String minShouldMatch = randomBoolean() ? null : "" + between(0, 1);
                Operator op = randomBoolean() ? Operator.AND : Operator.OR;
                MultiMatchQueryBuilder multiMatchQueryBuilder = randomBoolean() ?
                        multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category") :
                        multiMatchQuery("captain america", "*_name", randomBoolean() ? "category" : "categ*");
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(randomizeType(multiMatchQueryBuilder
                                .operator(op).tieBreaker(1.0f).minimumShouldMatch(minShouldMatch).type(type))).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(boolQuery().minimumShouldMatch(minShouldMatch)
                                .should(randomBoolean() ? termQuery("full_name", "captain america") :
                                    matchQuery("full_name", "captain america").operator(op))
                                .should(matchQuery("first_name", "captain america").operator(op))
                                .should(matchQuery("last_name", "captain america").operator(op))
                                .should(matchQuery("category", "captain america").operator(op))
                        ).get();
                assertEquivalent("captain america", left, right);
            }

            {
                String minShouldMatch = randomBoolean() ? null : "" + between(0, 1);
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(randomizeType(multiMatchQuery("capta", "full_name", "first_name", "last_name", "category")
                                .type(MatchQuery.Type.PHRASE_PREFIX).tieBreaker(1.0f).minimumShouldMatch(minShouldMatch))).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(boolQuery().minimumShouldMatch(minShouldMatch)
                                .should(matchPhrasePrefixQuery("full_name", "capta"))
                                .should(matchPhrasePrefixQuery("first_name", "capta"))
                                .should(matchPhrasePrefixQuery("last_name", "capta"))
                                .should(matchPhrasePrefixQuery("category", "capta"))
                        ).get();
                assertEquivalent("capta", left, right);
            }
            {
                String minShouldMatch = randomBoolean() ? null : "" + between(0, 1);
                SearchResponse left;
                if (randomBoolean()) {
                    left = client().prepareSearch("test").setSize(numDocs)
                            .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                            .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                                    .type(MatchQuery.Type.PHRASE).minimumShouldMatch(minShouldMatch))).get();
                } else {
                    left = client().prepareSearch("test").setSize(numDocs)
                            .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                            .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                                    .type(MatchQuery.Type.PHRASE).tieBreaker(1.0f).minimumShouldMatch(minShouldMatch))).get();
                }
                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
                        .addSort(SortBuilders.scoreSort()).addSort(SortBuilders.fieldSort("id"))
                        .setQuery(boolQuery().minimumShouldMatch(minShouldMatch)
                                .should(matchPhraseQuery("full_name", "captain america"))
                                .should(matchPhraseQuery("first_name", "captain america"))
                                .should(matchPhraseQuery("last_name", "captain america"))
                                .should(matchPhraseQuery("category", "captain america"))
                        ).get();
                assertEquivalent("captain america", left, right);
            }
        }
    }

    public void testCrossFieldMode() throws ExecutionException, InterruptedException {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theother"));
        assertSecondHit(searchResponse, hasId("theone"));
        assertThat(searchResponse.getHits().getHits()[0].getScore(), greaterThan(searchResponse.getHits().getHits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theother"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america 15", "full_name", "first_name", "last_name", "category", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .lenient(true)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america 15", "full_name", "first_name", "last_name", "category", "skill",
                        "int-field")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .lenient(true)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america 15", "skill", "full_name", "first_name", "last_name", "category",
                        "int-field")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .lenient(true)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));


        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america 15", "first_name", "last_name", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .lenient(true)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("25 15", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("25 15", "int-field", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("25 15", "first_name", "int-field", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("25 15", "int-field", "skill", "first_name")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("25 15", "int-field", "first_name", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category"))).get();
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .operator(Operator.OR))).get();
        assertFirstHit(searchResponse, hasId("theone"));


        // test group based on analyzer -- all fields are grouped into a cross field search
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .analyzer("category")
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));
        // counter example
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(randomBoolean() ? MultiMatchQueryBuilder.Type.CROSS_FIELDS : MultiMatchQueryBuilder.DEFAULT_TYPE)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 0L);

        // counter example
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("captain america marvel hero", "first_name", "last_name", "category")
                        .type(randomBoolean() ? MultiMatchQueryBuilder.Type.CROSS_FIELDS : MultiMatchQueryBuilder.DEFAULT_TYPE)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 0L);

        // test if boosts work
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("the ultimate", "full_name", "first_name", "category").field("last_name", 10)
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("ultimate1"));   // has ultimate in the last_name and that is boosted
        assertSecondHit(searchResponse, hasId("ultimate2"));
        assertThat(searchResponse.getHits().getHits()[0].getScore(), greaterThan(searchResponse.getHits().getHits()[1].getScore()));

        // since we try to treat the matching fields as one field scores are very similar but we have a small bias towards the
        // more frequent field that acts as a tie-breaker internally
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("the ultimate", "full_name", "first_name", "last_name", "category")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .operator(Operator.AND))).get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("ultimate2"));
        assertSecondHit(searchResponse, hasId("ultimate1"));
        assertThat(searchResponse.getHits().getHits()[0].getScore(), greaterThan(searchResponse.getHits().getHits()[1].getScore()));

        // Test group based on numeric fields
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "skill", "first_name")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        // Two numeric fields together caused trouble at one point!
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "int-field", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("15", "int-field", "first_name", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("alpha 15", "first_name", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .lenient(true))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("ultimate1"));
        /*
         * Doesn't find theone because "alpha 15" isn't a number and we don't
         * break on spaces.
         */
        assertHitCount(searchResponse, 1);

        // Lenient wasn't always properly lenient with two numeric fields
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("alpha 15", "int-field", "first_name", "skill")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                        .lenient(true))).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("ultimate1"));


        // Check that cross fields works with date fields
        searchResponse = client().prepareSearch("test")
                .setQuery(randomizeType(multiMatchQuery("now", "f*", "date")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)).lenient(true))
                .get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("nowHero"));
    }

    /**
     * Test for edge case where field level boosting is applied to field that doesn't exist on documents on
     * one shard. There was an issue reported in https://github.com/elastic/elasticsearch/issues/18710 where a
     * `multi_match` query using the fuzziness parameter with a boost on one of two fields returns the
     * same document score if both documents are placed on different shard. This test recreates that scenario
     * and checks that the returned scores are different.
     */
    public void testFuzzyFieldLevelBoosting() throws InterruptedException, ExecutionException {
        String idx = "test18710";
        CreateIndexRequestBuilder builder = prepareCreate(idx).setSettings(Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 3)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                );
        assertAcked(builder.setMapping("title", "type=text", "body", "type=text"));
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex(idx).setId("1").setSource(
                "title", "foo",
                "body", "bar"));
        builders.add(client().prepareIndex(idx).setId("2").setSource(
                "title", "bar",
                "body", "foo"));
        indexRandom(true, false, builders);

        SearchResponse searchResponse = client().prepareSearch(idx)
                .setExplain(true)
                .setQuery(multiMatchQuery("foo").field("title", 100).field("body")
                        .fuzziness(Fuzziness.ZERO)
                        ).get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertNotEquals("both documents should be on different shards", hits[0].getShard().getShardId(), hits[1].getShard().getShardId());
        assertEquals("1", hits[0].getId());
        assertEquals("2", hits[1].getId());
        assertThat(hits[0].getScore(), greaterThan(hits[1].getScore()));
    }

    private static void assertEquivalent(String query, SearchResponse left, SearchResponse right) {
        assertNoFailures(left);
        assertNoFailures(right);
        SearchHits leftHits = left.getHits();
        SearchHits rightHits = right.getHits();
        assertThat(leftHits.getTotalHits().value, equalTo(rightHits.getTotalHits().value));
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

    private static List<String> fill(List<String> list, String value, int times) {
        for (int i = 0; i < times; i++) {
            list.add(value);
        }
        return list;
    }

    private static List<String> fillRandom(List<String> list, int times) {
        for (int i = 0; i < times; i++) {
            list.add(randomAlphaOfLength(5));
        }
        return list;
    }

    private static <T> T randomPickExcept(List<T> fromList, T butNot) {
        while (true) {
            T t = RandomPicks.randomFrom(random(), fromList);
            if (t.equals(butNot)) {
                continue;
            }
            return t;
        }
    }

    private static MultiMatchQueryBuilder randomizeType(MultiMatchQueryBuilder builder) {
        try {
            MultiMatchQueryBuilder.Type type = builder.getType();
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
                            oType = MatchQuery.Type.BOOLEAN;
                        }
                        break;
                    case MOST_FIELDS:
                        if (randomBoolean()) {
                            oType = MatchQuery.Type.BOOLEAN;
                        }
                        break;
                    case CROSS_FIELDS:
                        break;
                    case PHRASE:
                        if (randomBoolean()) {
                            oType = MatchQuery.Type.PHRASE;
                        }
                        break;
                    case PHRASE_PREFIX:
                        if (randomBoolean()) {
                            oType = MatchQuery.Type.PHRASE_PREFIX;
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
}
