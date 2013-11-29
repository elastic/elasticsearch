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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

    public static void assertAcked(AcknowledgedRequestBuilder<?, ?, ?> builder) {
        assertAcked(builder.get());
    }

    public static void assertAcked(AcknowledgedResponse response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), equalTo(true));
        assertVersionSerializable(response);
    }

    public static void assertAcked(DeleteIndexRequestBuilder builder) {
        assertAcked(builder.get());
    }

    public static void assertAcked(CreateIndexRequestBuilder builder) {
        assertAcked(builder.get());
    }

    public static void assertAcked(DeleteIndexResponse response) {
        assertThat("Delete Index failed - not acked", response.isAcknowledged(), equalTo(true));
        assertVersionSerializable(response);
    }

    public static void assertAcked(CreateIndexResponse response) {
        assertThat("Create Index failed - not acked", response.isAcknowledged(), equalTo(true));
        assertVersionSerializable(response);
    }

    public static String formatShardStatus(BroadcastOperationResponse response) {
        String msg = " Total shards: " + response.getTotalShards() + " Successful shards: " + response.getSuccessfulShards() + " & "
                + response.getFailedShards() + " shard failures:";
        for (ShardOperationFailedException failure : response.getShardFailures()) {
            msg += "\n " + failure.toString();
        }
        return msg;
    }

    public static String formatShardStatus(SearchResponse response) {
        String msg = " Total shards: " + response.getTotalShards() + " Successful shards: " + response.getSuccessfulShards() + " & "
                + response.getFailedShards() + " shard failures:";
        for (ShardSearchFailure failure : response.getShardFailures()) {
            msg += "\n " + failure.toString();
        }
        return msg;
    }

    /*
     * assertions
     */
    public static void assertHitCount(SearchResponse searchResponse, long expectedHitCount) {
        if (searchResponse.getHits().totalHits() != expectedHitCount) {
            fail("Hit count is " + searchResponse.getHits().totalHits() + " but " + expectedHitCount + " was expected. "
                    + formatShardStatus(searchResponse));
        }
        assertVersionSerializable(searchResponse);
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        String shardStatus = formatShardStatus(searchResponse);
        assertThat("Expected different hit count. " + shardStatus, searchResponse.getHits().hits().length, equalTo(ids.length));

        Set<String> idsSet = new HashSet<String>(Arrays.asList(ids));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat("Expected id: " + hit.getId() + " in the result but wasn't." + shardStatus, idsSet.remove(hit.getId()),
                    equalTo(true));
        }
        assertThat("Expected ids: " + Arrays.toString(idsSet.toArray(new String[idsSet.size()])) + " in the result - result size differs."
                + shardStatus, idsSet.size(), equalTo(0));
        assertVersionSerializable(searchResponse);
    }

    public static void assertOrderedSearchHits(SearchResponse searchResponse, String... ids) {
        String shardStatus = formatShardStatus(searchResponse);
        assertThat("Expected different hit count. " + shardStatus, searchResponse.getHits().hits().length, equalTo(ids.length));
        for (int i = 0; i < ids.length; i++) {
            SearchHit hit = searchResponse.getHits().hits()[i];
            assertThat("Expected id: " + ids[i] + " at position " + i + " but wasn't." + shardStatus, hit.getId(), equalTo(ids[i]));
        }
        assertVersionSerializable(searchResponse);
    }

    public static void assertHitCount(CountResponse countResponse, long expectedHitCount) {
        if (countResponse.getCount() != expectedHitCount) {
            fail("Count is " + countResponse.getCount() + " but " + expectedHitCount + " was expected. " + formatShardStatus(countResponse));
        }
        assertVersionSerializable(countResponse);
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
        assertVersionSerializable(searchResponse);
    }

    public static void assertNoFailures(SearchResponse searchResponse) {
        assertThat("Unexpected ShardFailures: " + Arrays.toString(searchResponse.getShardFailures()),
                searchResponse.getShardFailures().length, equalTo(0));
        assertVersionSerializable(searchResponse);
    }

    public static void assertNoFailures(BroadcastOperationResponse response) {
        assertThat("Unexpected ShardFailures: " + Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));
        assertVersionSerializable(response);
    }

    public static void assertSearchHit(SearchHit searchHit, Matcher<SearchHit> matcher) {
        assertThat(searchHit, matcher);
        assertVersionSerializable(searchHit);
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, Matcher<String> matcher) {
        assertHighlight(resp, hit, field, fragment, greaterThan(fragment), matcher);
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, int totalFragments, Matcher<String> matcher) {
        assertHighlight(resp, hit, field, fragment, equalTo(totalFragments), matcher);
    }

    public static void assertHighlight(SearchHit hit, String field, int fragment, Matcher<String> matcher) {
        assertHighlight(hit, field, fragment, greaterThan(fragment), matcher);
    }

    public static void assertHighlight(SearchHit hit, String field, int fragment, int totalFragments, Matcher<String> matcher) {
        assertHighlight(hit, field, fragment, equalTo(totalFragments), matcher);
    }

    private static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, Matcher<Integer> fragmentsMatcher, Matcher<String> matcher) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().hits().length, greaterThan(hit));
        assertHighlight(resp.getHits().hits()[hit], field, fragment, fragmentsMatcher, matcher);
        assertVersionSerializable(resp);
    }

    private static void assertHighlight(SearchHit hit, String field, int fragment, Matcher<Integer> fragmentsMatcher, Matcher<String> matcher) {
        assertThat(hit.getHighlightFields(), hasKey(field));
        assertThat(hit.getHighlightFields().get(field).fragments().length, fragmentsMatcher);
        assertThat(hit.highlightFields().get(field).fragments()[fragment].string(), matcher);
    }

    public static void assertNotHighlighted(SearchResponse resp, int hit, String field) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().hits().length, greaterThan(hit));
        assertThat(resp.getHits().hits()[hit].getHighlightFields(), not(hasKey(field)));
    }

    public static void assertSuggestionSize(Suggest searchSuggest, int entry, int size, String key) {
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), equalTo(size));
        assertVersionSerializable(searchSuggest);
    }

    public static void assertSuggestion(Suggest searchSuggest, int entry, int ord, String key, String text) {
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), greaterThan(ord));
        assertThat(searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().get(ord).getText().string(), equalTo(text));
        assertVersionSerializable(searchSuggest);
    }

    /**
     * Assert suggestion returns exactly the provided text.
     */
    public static void assertSuggestion(Suggest searchSuggest, int entry, String key, String... text) {
        assertSuggestion(searchSuggest, entry, key, text.length, text);
    }

    /**
     * Assert suggestion returns size suggestions and the first are the provided
     * text.
     */
    public static void assertSuggestion(Suggest searchSuggest, int entry, String key, int size, String... text) {
        assertSuggestionSize(searchSuggest, entry, size, key);
        for (int i = 0; i < text.length; i++) {
            assertSuggestion(searchSuggest, entry, i, key, text[i]);
        }
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

    public static Matcher<SearchHit> hasScore(final float score) {
        return new ElasticsearchMatchers.SearchHitHasScoreMatcher(score);
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

    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, Class<E> exceptionClass, String extraInfo) {
        assertThrows(builder.execute(), exceptionClass, extraInfo);
    }

    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass) {
        assertThrows(future, exceptionClass, null);
    }

    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass, String extraInfo) {
        boolean fail = false;
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + exceptionClass + " exception to be thrown";

        try {
            future.actionGet();
            fail = true;

        } catch (ElasticSearchException esException) {
            assertThat(extraInfo, esException.unwrapCause(), instanceOf(exceptionClass));
        } catch (Throwable e) {
            assertThat(extraInfo, e, instanceOf(exceptionClass));
        }
        // has to be outside catch clause to get a proper message
        if (fail) {
            throw new AssertionError(extraInfo);
        }
    }

    private static BytesReference serialize(Version version, Streamable streamable) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        streamable.writeTo(output);
        output.flush();
        return output.bytes();
    }

    public static void assertVersionSerializable(Streamable streamable) {
        assert Version.CURRENT.after(ElasticsearchTestCase.getPreviousVersion());
        assertVersionSerializable(ElasticsearchTestCase.randomVersion(), streamable);
    }

    public static void assertVersionSerializable(Version version, Streamable streamable) {
        try {
            Streamable newInstance = tryCreateNewInstance(streamable);
            if (newInstance == null) {
                return; // can't create a new instance - we never modify a
                        // streamable that comes in.
            }
            if (streamable instanceof ActionRequest) {
                ((ActionRequest<?>)streamable).validate();
            }
            BytesReference orig = serialize(version, streamable);
            StreamInput input = new BytesStreamInput(orig);
            input.setVersion(version);
            newInstance.readFrom(input);
            assertThat("Stream should be fully read with version [" + version + "] for streamable [" + streamable + "]", input.available(), equalTo(0));
            assertThat("Serialization failed with version [" + version + "] bytes should be equal for streamable [" + streamable + "]", serialize(version, streamable), equalTo(orig));
        } catch (Throwable ex) {
            throw new RuntimeException("failed to check serialization - version [" + version + "] for streamable [" + streamable + "]", ex);
        }

    }

    private static Streamable tryCreateNewInstance(Streamable streamable) throws NoSuchMethodException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        try {
            Class<? extends Streamable> clazz = streamable.getClass();
            Constructor<? extends Streamable> constructor = clazz.getDeclaredConstructor();
            assertThat(constructor, Matchers.notNullValue());
            constructor.setAccessible(true);
            Streamable newInstance = constructor.newInstance();
            return newInstance;
        } catch (Throwable e) {
            return null;
        }
    }

}
