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

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

/**
 *
 */
public class ElasticsearchAssertions {

    /*
     * assertions
     */
    public static void assertHitCount(SearchResponse searchResponse, long expectedHitCount) {
        if (searchResponse.getHits().totalHits() != expectedHitCount) {
            String msg = "Hit count is " + searchResponse.getHits().totalHits() + " but " + expectedHitCount + " was expected. " +
                    searchResponse.getFailedShards() + " shard failures:";
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                msg += "\n " + failure.toString();
            }
            fail(msg);
        }
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        assertThat("Expected different hit count", searchResponse.getHits().hits().length, equalTo(ids.length));

        Set<String> idsSet = new HashSet<String>(Arrays.asList(ids));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat("Expected id: " + hit.getId() + " in the result but wasn't", idsSet.remove(hit.getId()), equalTo(true));
        }
        assertThat("Expected ids: " + Arrays.toString(idsSet.toArray(new String[0])) + " in the result - result size differs", idsSet.size(), equalTo(0));
    }

    public static void assertHitCount(CountResponse countResponse, long expectedHitCount) {
        if (countResponse.getCount() != expectedHitCount) {
            String msg = "Count is " + countResponse.getCount() + " but " + expectedHitCount + " was expected. " +
                    countResponse.getFailedShards() + " shard failures:";
            for (ShardOperationFailedException failure : countResponse.getShardFailures()) {
                msg += "\n " + failure.toString();
            }
            fail(msg);
        }
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
        assertThat(searchResponse.getHits().totalHits(), greaterThanOrEqualTo((long) number));
        assertSearchHit(searchResponse.getHits().getAt(number - 1), matcher);
    }

    public static void assertNoFailures(SearchResponse searchResponse) {
        assertThat("Unexpectd ShardFailures: " + Arrays.toString(searchResponse.getShardFailures()), searchResponse.getShardFailures().length, equalTo(0));
    }

    public static void assertNoFailures(BroadcastOperationResponse response) {
        assertThat("Unexpectd ShardFailures: " + Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));
    }

    public static void assertSearchHit(SearchHit searchHit, Matcher<SearchHit> matcher) {
        assertThat(searchHit, matcher);
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, Matcher<String> matcher) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().hits().length, greaterThan(hit));
        assertThat(resp.getHits().hits()[hit].getHighlightFields().get(field), notNullValue());
        assertThat(resp.getHits().hits()[hit].getHighlightFields().get(field).fragments().length, greaterThan(fragment));
        assertThat(resp.getHits().hits()[hit].highlightFields().get(field).fragments()[fragment].string(), matcher);
    }

    public static void assertSuggestionSize(Suggest searchSuggest, int entry, int size, String key) {
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), equalTo(size));

    }

    public static void assertSuggestion(Suggest searchSuggest, int entry, int ord, String key, String text) {
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), greaterThan(ord));
        assertThat(searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().get(ord).getText().string(), equalTo(text));
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

    public static <T extends Query> T assertBooleanSubQuery(Query query, Class<T> subqueryType, int i) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery q = (BooleanQuery) query;
        assertThat(q.getClauses().length, greaterThan(i));
        assertThat(q.getClauses()[i].getQuery(), instanceOf(subqueryType));
        return (T) q.getClauses()[i].getQuery();
    }

    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, Class<E> exceptionClass) {
        assertThrows(builder.execute(), exceptionClass);
    }

    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass) {
        boolean fail = false;
        try {
            future.actionGet();
            fail = true;

        } catch (ElasticSearchException esException) {
            assertThat(esException.unwrapCause(), instanceOf(exceptionClass));
        } catch (Throwable e) {
            assertThat(e, instanceOf(exceptionClass));
        }
        // has to be outside catch clause to get a proper message
        if (fail) {
            throw new AssertionError("Expected a " + exceptionClass + " exception to be thrown");
        }
    }

}
