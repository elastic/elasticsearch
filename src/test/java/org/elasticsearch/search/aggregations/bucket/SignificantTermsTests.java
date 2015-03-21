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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class SignificantTermsTests extends ElasticsearchIntegrationTest {

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    public static final int MUSIC_CATEGORY=1;
    public static final int OTHER_CATEGORY=2;
    public static final int SNOWBOARDING_CATEGORY=3;
    
    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 5, SETTING_NUMBER_OF_REPLICAS, 0).addMapping("fact",
                "_routing", "required=true", "routing_id", "type=string,index=not_analyzed", "fact_category",
                "type=integer,index=not_analyzed", "description", "type=string,index=analyzed"));
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
    }

    @Test
    public void structuredAnalysis() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("fact_category").executionHint(randomExecutionHint())
                           .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        Number topCategory = (Number) topTerms.getBuckets().iterator().next().getKey();
        assertTrue(topCategory.equals(new Long(SNOWBOARDING_CATEGORY)));
    }
    
    @Test
    public void structuredAnalysisWithIncludeExclude() throws Exception {
        long[] excludeTerms = { MUSIC_CATEGORY };
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "paul"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("fact_category").executionHint(randomExecutionHint())
                           .minDocCount(1).exclude(excludeTerms))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        Number topCategory = (Number) topTerms.getBuckets().iterator().next().getKey();
        assertTrue(topCategory.equals(new Long(OTHER_CATEGORY)));
    }

    @Test
    public void includeExclude() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .exclude("weller"))
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
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .include("weller"))
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
    
    @Test
    public void includeExcludeExactValues() throws Exception {
        String []incExcTerms={"weller","nosuchterm"};
        SearchResponse response = client().prepareSearch("test")
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .exclude(incExcTerms))
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
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                        .include(incExcTerms))
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
    
    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("fact_category").executionHint(randomExecutionHint())
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");        
        assertThat(topTerms.getBuckets().size(), equalTo(0));
    }

    @Test
    public void textAnalysis() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                           .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    @Test
    public void textAnalysisGND() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint()).significanceHeuristic(new GND.GNDBuilder(true))
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    @Test
    public void textAnalysisChiSquare() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint()).significanceHeuristic(new ChiSquare.ChiSquareBuilder(false,true))
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    @Test
    public void textAnalysisPercentageScore() throws Exception {
        SearchResponse response = client()
                .prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0)
                .setSize(60)
                .setExplain(true)
                .addAggregation(
                        new SignificantTermsBuilder("mySignificantTerms").field("description").executionHint(randomExecutionHint())
                                .significanceHeuristic(new PercentageScore.PercentageScoreBuilder()).minDocCount(2)).execute().actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    @Test
    public void badFilteredAnalysis() throws Exception {
        // Deliberately using a bad choice of filter here for the background context in order
        // to test robustness. 
        // We search for the name of a snowboarder but use music-related content (fact_category:1)
        // as the background source of term statistics.
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)                
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description")
                           .minDocCount(2).backgroundFilter(FilterBuilders.termFilter("fact_category", 1)))
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
    
    
    @Test
    public void filteredAnalysis() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "weller"))
                .setFrom(0).setSize(60).setExplain(true)                
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description")
                           .minDocCount(1).backgroundFilter(FilterBuilders.termsFilter("description",  "paul")))
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

    @Test
    public void nestedAggs() throws Exception {
        String[][] expectedKeywordsByCategory={
                { "paul", "weller", "jam", "style", "council" },                
                { "paul", "smith" },
                { "craig", "kelly", "terje", "haakonsen", "burton" }};
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .addAggregation(new TermsBuilder("myCategories").field("fact_category").minDocCount(2)
                        .subAggregation(
                                   new SignificantTermsBuilder("mySignificantTerms").field("description")
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


    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped", "test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms").field("description")
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
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms")
                        .field("description")
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(new JLHScore.JLHScoreBuilder())
                        .minDocCount(2))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }

    @Test
    public void testMutualInformation() throws Exception {
        SearchResponse response = client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(new TermQueryBuilder("_all", "terje"))
                .setFrom(0).setSize(60).setExplain(true)
                .addAggregation(new SignificantTermsBuilder("mySignificantTerms")
                        .field("description")
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(new MutualInformation.MutualInformationBuilder(false, true))
                        .minDocCount(1))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        SignificantTerms topTerms = response.getAggregations().get("mySignificantTerms");
        checkExpectedStringTermsFound(topTerms);
    }
}
