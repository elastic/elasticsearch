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

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ESIntegTestCase.SuiteScopeTestCase
public class SignificantTermsIT extends ESIntegTestCase {

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", numberOfReplicas())
                .build();
    }

    public static final int MUSIC_CATEGORY=1;
    public static final int OTHER_CATEGORY=2;
    public static final int SNOWBOARDING_CATEGORY=3;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 5, SETTING_NUMBER_OF_REPLICAS, 0).addMapping("fact",
                "_routing", "required=true", "routing_id", "type=keyword", "fact_category",
                "type=integer", "description", "type=text,fielddata=true"));
        createIndex("idx_unmapped");

        ensureGreen();
        String data[] = {
                    "A\t1\tpaul weller was lead singer of the jam before the style council",
                    "B\t1\tpaul weller left the jam to form the style council",
                    "A\t2\tpaul smith is a designer in the fashion industry",
                    "B\t1\tthe stranglers are a group originally from guildford",
                    "A\t1\tafter disbanding the style council in 1985 paul weller became a solo artist",
                    "B\t1\tjean jaques burnel is a bass player in the stranglers and has a black belt in karate",
                    "A\t1\tmalcolm owen was the lead singer of the ruts",
                    "B\t1\tpaul weller has denied any possibility of a reunion of the jam",
                    "A\t1\tformer frontman of the jam paul weller became the father of twins",
                    "B\t2\tex-england football star paul gascoigne has re-emerged following recent disappearance",
                    "A\t2\tdavid smith has recently denied connections with the mafia",
                    "B\t1\tthe damned's new rose single was considered the first 'punk' single in the UK",
                    "A\t1\tthe sex pistols broke up after a few short years together",
                    "B\t1\tpaul gascoigne was a midfielder for england football team",
                    "A\t3\tcraig kelly became the first world champion snowboarder and has a memorial at baldface lodge",
                    "B\t3\tterje haakonsen has credited craig kelly as his snowboard mentor",
                    "A\t3\tterje haakonsen and craig kelly were some of the first snowboarders sponsored by burton snowboards",
                    "B\t3\tlike craig kelly before him terje won the mt baker banked slalom many times - once riding switch",
                    "A\t3\tterje haakonsen has been a team rider for burton snowboards for over 20 years"
            };

        for (int i = 0; i < data.length; i++) {
            String[] parts = data[i].split("\t");
            client().prepareIndex("test", "fact", "" + i)
                    .setRouting(parts[0])
                    .setSource("fact_category", parts[1], "description", parts[2]).get();
        }
        client().admin().indices().refresh(new RefreshRequest("test")).get();

        assertAcked(prepareCreate("test_not_indexed")
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .addMapping("type",
                        "my_keyword", "type=keyword,index=false",
                        "my_long", "type=long,index=false"));
        indexRandom(true,
                client().prepareIndex("test_not_indexed", "type", "1").setSource(
                        "my_keyword", "foo", "my_long", 42));
    }

    public void testStructuredAnalysis() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("fact_category").executionHint(randomExecutionHint())
                           .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        Number topCategory = (Number) topTerms.getBuckets().iterator().next().getKey();
        assertTrue(topCategory.equals(Long.valueOf(SNOWBOARDING_CATEGORY)));
    }

    public void testStructuredAnalysisWithIncludeExclude() throws Exception {
        long[] excludeTerms = { MUSIC_CATEGORY };
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "paul"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("fact_category").executionHint(randomExecutionHint())
                           .minDocCount(1).includeExclude(new IncludeExclude(null, excludeTerms)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        Number topCategory = (Number) topTerms.getBuckets().iterator().next().getKey();
        assertTrue(topCategory.equals(Long.valueOf(OTHER_CATEGORY)));
    }

    public void testIncludeExclude() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .includeExclude(new IncludeExclude(null, "weller")))
                .get();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        Set<String> terms  = new HashSet<>();
        for (Bucket topTerm : topTerms) {
            terms.add(topTerm.getKeyAsString());
        }
        assertThat(terms, hasSize(6));
        assertThat(terms.contains("jam"), is(true));
        assertThat(terms.contains("council"), is(true));
        assertThat(terms.contains("style"), is(true));
        assertThat(terms.contains("paul"), is(true));
        assertThat(terms.contains("of"), is(true));
        assertThat(terms.contains("the"), is(true));

        response = client().prepareSearch("test")
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .includeExclude(new IncludeExclude("weller", null)))
                .get();
        assertSearchResponse(response);
        topTerms = response.getAggregations().get("mySignificantTerms");
        terms  = new HashSet<>();
        for (Bucket topTerm : topTerms) {
            terms.add(topTerm.getKeyAsString());
        }
        assertThat(terms, hasSize(1));
        assertThat(terms.contains("weller"), is(true));
    }

    public void testIncludeExcludeExactValues() throws Exception {
        String []incExcTerms={"weller","nosuchterm"};
        SearchResponse response = client().prepareSearch("test")
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .includeExclude(new IncludeExclude(null, incExcTerms)))
                .get();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        Set<String> terms  = new HashSet<>();
        for (Bucket topTerm : topTerms) {
            terms.add(topTerm.getKeyAsString());
        }
        assertEquals(new HashSet<String>(Arrays.asList("jam", "council", "style", "paul", "of", "the")), terms);

        response = client().prepareSearch("test")
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .includeExclude(new IncludeExclude(incExcTerms, null)))
                .get();
        assertSearchResponse(response);
        topTerms = response.getAggregations().get("mySignificantTerms");
        terms  = new HashSet<>();
        for (Bucket topTerm : topTerms) {
            terms.add(topTerm.getKeyAsString());
        }
        assertThat(terms, hasSize(1));
        assertThat(terms.contains("weller"), is(true));
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("fact_category").executionHint(randomExecutionHint())
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        assertThat(topTerms.getBuckets().size(), equalTo(0));
    }

    public void testTextAnalysis() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                           .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    public void testTextAnalysisGND() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint()).significanceHeuristic(new GND(true))
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    public void testTextAnalysisChiSquare() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint()).significanceHeuristic(new ChiSquare(false,true))
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    public void testTextAnalysisPercentageScore() throws Exception {
        SearchResponse response = client()
                .prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0)
                .setSize(60)
                .setExplain(true)
                .addAggregation(
                        significantTerms("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                                .significanceHeuristic(new PercentageScore()).minDocCount(2)).execute().actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    public void testBadFilteredAnalysis() throws Exception {
        // Deliberately using a bad choice of filter here for the background context in order
        // to test robustness.
        // We search for the name of a snowboarder but use music-related content (fact_category:1)
        // as the background source of term statistics.
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("description")
                           .minDocCount(2).backgroundFilter(QueryBuilders.termQuery("fact_category", 1)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        // We expect at least one of the significant terms to have been selected on the basis
        // that it is present in the foreground selection but entirely missing from the filtered
        // background used as context.
        boolean hasMissingBackgroundTerms = false;
        for (Bucket topTerm : topTerms) {
            if (topTerm.getSupersetDf() == 0) {
                hasMissingBackgroundTerms = true;
                break;
            }
        }
        assertTrue(hasMissingBackgroundTerms);
    }

    public void testFilteredAnalysis() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("description")
                           .minDocCount(1).backgroundFilter(QueryBuilders.termsQuery("description",  "paul")))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        HashSet<String> topWords = new HashSet<String>();
        for (Bucket topTerm : topTerms) {
            topWords.add(topTerm.getKeyAsString());
        }
        //The word "paul" should be a constant of all docs in the background set and therefore not seen as significant
        assertFalse(topWords.contains("paul"));
        //"Weller" is the only Paul who was in The Jam and therefore this should be identified as a differentiator from the background of all other Pauls.
        assertTrue(topWords.contains("jam"));
    }

    public void testNestedAggs() throws Exception {
        String[][] expectedKeywordsByCategory={
                { "paul", "weller", "jam", "style", "council" },
                { "paul", "smith" },
                { "craig", "kelly", "terje", "haakonsen", "burton" }};
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .addAggregation(terms("myCategories").field("fact_category").minDocCount(2)
                        .subAggregation(
                                   significantTerms("mySignificantTerms").field("description")
                                   .executionHint(randomExecutionHint())
                                   .minDocCount(2)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        Terms topCategoryTerms = response.getAggregations().get("myCategories");
        for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket topCategory : topCategoryTerms.getBuckets()) {
            SignificantTerms topTerms = topCategory.getAggregations().get("mySignificantTerms");
            HashSet<String> foundTopWords = new HashSet<String>();
            for (Bucket topTerm : topTerms) {
                foundTopWords.add(topTerm.getKeyAsString());
            }
            String[] expectedKeywords = expectedKeywordsByCategory[Integer.parseInt(topCategory.getKeyAsString()) - 1];
            for (String expectedKeyword : expectedKeywords) {
                assertTrue(expectedKeyword + " missing from category keywords", foundTopWords.contains(expectedKeyword));
            }
        }
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped", "test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms").field("description")
                            .executionHint(randomExecutionHint())
                           .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }


    private void checkExpectedStringTermsFound(SignificantTerms topTerms) {
        HashMap<String,Bucket>topWords=new HashMap<>();
        for (Bucket topTerm : topTerms ){
            topWords.put(topTerm.getKeyAsString(), topTerm);
        }
        assertTrue( topWords.containsKey("haakonsen"));
        assertTrue( topWords.containsKey("craig"));
        assertTrue( topWords.containsKey("kelly"));
        assertTrue( topWords.containsKey("burton"));
        assertTrue( topWords.containsKey("snowboards"));
        Bucket kellyTerm=topWords.get("kelly");
        assertEquals(3, kellyTerm.getSubsetDf());
        assertEquals(4, kellyTerm.getSupersetDf());
    }

    public void testDefaultSignificanceHeuristic() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms")
                        .field("description")
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(new JLHScore())
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    public void testMutualInformation() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(significantTerms("mySignificantTerms")
                        .field("description")
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(new MutualInformation(false, true))
                        .minDocCount(1))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    public void testFailIfFieldNotIndexed() {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test_not_indexed").addAggregation(
                        significantTerms("mySignificantTerms").field("my_keyword")).get());
        assertThat(e.toString(),
                containsString("Cannot search on field [my_keyword] since it is not indexed."));

        e = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test_not_indexed").addAggregation(
                        significantTerms("mySignificantTerms").field("my_long")).get());
        assertThat(e.toString(),
                containsString("Cannot search on field [my_long] since it is not indexed."));
    }
}
