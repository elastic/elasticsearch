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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
        int numDocs = atLeast(50);
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
        builders.add(client().prepareIndex("test", "test", "theone").setSource(
                "full_name", "Captain America",
                "first_name", "Captain",
                "last_name", "America",
                "category", "marvel hero"));
        builders.add(client().prepareIndex("test", "test", "theother").setSource(
                "full_name", "marvel hero",
                "first_name", "marvel",
                "last_name", "hero",
                "category", "bogus"));

        builders.add(client().prepareIndex("test", "test", "ultimate1").setSource(
                "full_name", "Alpha the Ultimate Mutant",
                "first_name", "Alpha the",
                "last_name", "Ultimate Mutant",
                "category", "marvel hero"));
        builders.add(client().prepareIndex("test", "test", "ultimate2").setSource(
                "full_name", "Man the Ultimate Ninja",
                "first_name", "Man the Ultimate",
                "last_name", "Ninja",
                "category", "marvel hero"));

        builders.add(client().prepareIndex("test", "test", "anotherhero").setSource(
                "full_name", "ultimate",
                "first_name", "wolferine",
                "last_name", "",
                "category", "marvel hero"));
        List<String> firstNames = new ArrayList<String>();
        fill(firstNames, "Captain", between(15, 25));
        fill(firstNames, "Ultimate", between(5, 10));
        fillRandom(firstNames, between(3, 7));
        List<String> lastNames = new ArrayList<String>();
        fill(lastNames, "Captain", between(3, 7));
        fillRandom(lastNames, between(30, 40));
        for (int i = 0; i < numDocs; i++) {
            String first = RandomPicks.randomFrom(getRandom(), firstNames);
            String last = randomPickExcept(lastNames, first);
            builders.add(client().prepareIndex("test", "test", "" + i).setSource(
                    "full_name", first + " " + last,
                    "first_name", first,
                    "last_name", last,
                    "category", randomBoolean() ? "marvel hero" : "bogus"));
        }
        indexRandom(true, builders);
    }

    @Test
    public void testDefaults() throws ExecutionException, InterruptedException {
        MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR)).get();
        Set<String> topNIds = Sets.newHashSet("theone", "theother");
        for (int i = 0; i < searchResponse.getHits().hits().length; i++) {
            topNIds.remove(searchResponse.getHits().getAt(i).getId());
            // very likely that we hit a random doc that has the same score so orders are random since
            // the doc id is the tie-breaker
        }
        assertThat(topNIds, empty());
        assertThat(searchResponse.getHits().hits()[0].getScore(), equalTo(searchResponse.getHits().hits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).useDisMax(false).type(type)).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).type(type)).get();
        assertFirstHit(searchResponse, hasId("theother"));


        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).type(type)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).type(type)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));
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

    public void testPhraseType() {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("Man the Ultimate", "full_name_phrase", "first_name_phrase", "last_name_phrase", "category_phrase")
                        .operator(MatchQueryBuilder.Operator.OR).type(MatchQueryBuilder.Type.PHRASE)).get();
        assertFirstHit(searchResponse, hasId("ultimate2"));
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("Captain", "full_name_phrase", "first_name_phrase", "last_name_phrase", "category_phrase")
                        .operator(MatchQueryBuilder.Operator.OR).type(MatchQueryBuilder.Type.PHRASE)).get();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("the Ul", "full_name_phrase", "first_name_phrase", "last_name_phrase", "category_phrase")
                        .operator(MatchQueryBuilder.Operator.OR).type(MatchQueryBuilder.Type.PHRASE_PREFIX)).get();
        assertFirstHit(searchResponse, hasId("ultimate2"));
        assertSecondHit(searchResponse, hasId("ultimate1"));
        assertHitCount(searchResponse, 2l);
    }

    @Test
    public void testCutoffFreq() throws ExecutionException, InterruptedException {
        final long numDocs = client().prepareCount("test")
                .setQuery(matchAllQuery()).get().getCount();
        MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
        Float cutoffFrequency = randomBoolean() ? Math.min(1, numDocs * 1.f / between(10, 20)) : 1.f / between(10, 20);
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).cutoffFrequency(cutoffFrequency)).get();
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
                .setQuery(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).useDisMax(false).cutoffFrequency(cutoffFrequency).type(type)).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat(searchResponse.getHits().hits()[0].getScore(), greaterThan(searchResponse.getHits().hits()[1].getScore()));
        long size = searchResponse.getHits().getTotalHits();

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).useDisMax(false).type(type)).get();
        assertFirstHit(searchResponse, anyOf(hasId("theone"), hasId("theother")));
        assertThat("common terms expected to be a way smaller result set", size, lessThan(searchResponse.getHits().getTotalHits()));

        cutoffFrequency = randomBoolean() ? Math.min(1, numDocs * 1.f / between(10, 20)) : 1.f / between(10, 20);
        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("marvel hero", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.OR).cutoffFrequency(cutoffFrequency).type(type)).get();
        assertFirstHit(searchResponse, hasId("theother"));


        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).cutoffFrequency(cutoffFrequency).type(type)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));

        searchResponse = client().prepareSearch("test")
                .setQuery(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                        .operator(MatchQueryBuilder.Operator.AND).cutoffFrequency(cutoffFrequency).type(type)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("theone"));
    }


    public void testEquivalence() {

        final int numDocs = (int) client().prepareCount("test")
                .setQuery(matchAllQuery()).get().getCount();
        int numIters = atLeast(5);
        for (int i = 0; i < numIters; i++) {
            {
                MatchQueryBuilder.Type type = randomBoolean() ? null : MatchQueryBuilder.Type.BOOLEAN;
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .setQuery(multiMatchQuery("marvel hero captain america", "full_name", "first_name", "last_name", "category")
                                .operator(MatchQueryBuilder.Operator.OR).type(type)).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
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
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .setQuery(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                                .operator(op).useDisMax(false).minimumShouldMatch(minShouldMatch).type(type)).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
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
                        .setQuery(multiMatchQuery("capta", "full_name", "first_name", "last_name", "category")
                                .type(MatchQueryBuilder.Type.PHRASE_PREFIX).useDisMax(false).minimumShouldMatch(minShouldMatch)).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
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
                SearchResponse left = client().prepareSearch("test").setSize(numDocs)
                        .setQuery(multiMatchQuery("captain america", "full_name", "first_name", "last_name", "category")
                                .type(MatchQueryBuilder.Type.PHRASE).useDisMax(false).minimumShouldMatch(minShouldMatch)).get();

                SearchResponse right = client().prepareSearch("test").setSize(numDocs)
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
            assertThat("query: " + query + " hit: " + i, (double)hits[i].getScore(), closeTo(rHits[i].getScore(), 0.00001d));
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
}
