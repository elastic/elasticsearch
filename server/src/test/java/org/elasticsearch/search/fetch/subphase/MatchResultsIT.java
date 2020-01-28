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

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.fetch.subphase.matches.MatchesContextBuilder;
import org.elasticsearch.search.fetch.subphase.matches.MatchingTerms;
import org.elasticsearch.search.fetch.subphase.matches.MatchingTermsProcessor;
import org.elasticsearch.search.fetch.subphase.matches.NamedQueries;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;


public class MatchResultsIT extends ESIntegTestCase {

    public void testNamedQueries() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test1").get();
        client().prepareIndex("test").setId("2").setSource("name", "test2").get();
        client().prepareIndex("test").setId("3").setSource("name", "test3").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(boolQuery()
                .should(boolQuery()
                    .must(termQuery("name", "test1").queryName("test1"))
                    .must(termQuery("name", "test2"))
                )
                .should(termQuery("name", "test3").queryName("test3"))
            ).get();

        NamedQueries names = response.getHits().getAt(0).getMatchesResult(NamedQueries.class);
        assertNotNull(names);
        assertThat(names.getNamedQueries(), contains("test3"));
    }

    public void testMatchingTerms() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test1 test4").get();
        client().prepareIndex("test").setId("2").setSource("name", "test2").get();
        client().prepareIndex("test").setId("3").setSource("name", "test3").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(boolQuery()
                .should(queryStringQuery("test1 test4 test5"))
                .should(matchPhraseQuery("name", "test1 test2")))
            .matchDetails(new MatchesContextBuilder(Map.of(MatchingTermsProcessor.NAME, Settings.EMPTY)))
            .get();

        MatchingTerms matches = response.getHits().getAt(0).getMatchesResult(MatchingTerms.class);
        assertNotNull(matches);
        assertThat(matches.matchedFields(), contains("name"));
        assertThat(matches.matches("name"), contains("test1", "test4"));

        response = client().prepareSearch("test")
            .setQuery(boolQuery()
                .should(queryStringQuery("test1 test4 test5").queryName("querystring"))
                .should(matchPhraseQuery("name", "test1 test2").queryName("phrase")))
            .matchDetails(MatchingTermsProcessor.NAME, "named_queries", "true")
            .get();

        matches = response.getHits().getAt(0).getMatchesResult(MatchingTerms.class);
        assertNotNull(matches);
        assertThat(matches.matchedFields(), contains("name"));
        assertThat(matches.matches("name"), contains("test1", "test4"));
        assertThat(matches.matchedFields("querystring"), contains("name"));
        assertThat(matches.matchedFields("phrasequery"), hasSize(0));
        assertThat(matches.matches("querystring", "name"), contains("test1", "test4"));
    }

}
