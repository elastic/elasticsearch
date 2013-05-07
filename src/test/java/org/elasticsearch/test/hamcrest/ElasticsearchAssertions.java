/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.test.hamcrest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.hamcrest.Matcher;

/**
 *
 */
public class ElasticsearchAssertions {

    /*
     * assertions
     */
    public static void assertHitCount(SearchResponse searchResponse, long expectedHitCount) {
        assertThat(searchResponse.getHits().totalHits(), is(expectedHitCount));
    }

    public static void assertFirstHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 1, matcher);
    }

    public static void assertSecondHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 2, matcher);
    }

    public static void assertThirdHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 3, matcher);
    }

    public static void assertSearchHit(SearchResponse searchResponse, int number, Matcher<SearchHit> matcher) {
        assert number > 0;
        assertThat("SearchHit number must be greater than 0", number, greaterThan(0));
        assertThat(searchResponse.getHits().totalHits(), greaterThanOrEqualTo((long)number));
        assertSearchHit(searchResponse.getHits().getAt(number-1), matcher);
    }

    public static void assertSearchHit(SearchHit searchHit, Matcher<SearchHit> matcher) {
        assertThat(searchHit, matcher);
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, Matcher<String> matcher) {
        assertThat("Unexpectd ShardFailures: " + Arrays.toString(resp.getShardFailures()), resp.getShardFailures().length, equalTo(0));
        assertThat("not enough hits", resp.getHits().hits().length, greaterThan(hit));
        assertThat(resp.getHits().hits()[hit].getHighlightFields().get(field), notNullValue());
        assertThat(resp.getHits().hits()[hit].getHighlightFields().get(field).fragments().length, greaterThan(fragment));
        assertThat(resp.getHits().hits()[hit].highlightFields().get(field).fragments()[fragment].string(), matcher);
    }

    /*
     * matchers
     */
    public static Matcher<SearchHit> hasId(final String id) {
        return new ElasticsearchMatchers.SearchHitHasIdMatcher(id);
    }

    public static Matcher<SearchHit> hasType(final String type) {
        return new ElasticsearchMatchers.SearchHitHasTypeMatcher(type);
    }

    public static Matcher<SearchHit> hasIndex(final String index) {
        return new ElasticsearchMatchers.SearchHitHasIndexMatcher(index);
    }

}
