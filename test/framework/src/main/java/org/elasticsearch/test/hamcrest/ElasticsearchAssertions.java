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
package org.elasticsearch.test.hamcrest;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticsearchAssertions {

    public static void assertAcked(AcknowledgedRequestBuilder<?, ?, ?> builder) {
        assertAcked(builder.get());
    }

    public static void assertNoTimeout(ClusterHealthRequestBuilder requestBuilder) {
        assertNoTimeout(requestBuilder.get());
    }

    public static void assertNoTimeout(ClusterHealthResponse response) {
        assertThat("ClusterHealthResponse has timed out - returned: [" + response + "]", response.isTimedOut(), is(false));
    }

    public static void assertAcked(AcknowledgedResponse response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), equalTo(true));
        assertVersionSerializable(response);
    }

    public static void assertAcked(DeleteIndexRequestBuilder builder) {
        assertAcked(builder.get());
    }

    public static void assertAcked(DeleteIndexResponse response) {
        assertThat("Delete Index failed - not acked", response.isAcknowledged(), equalTo(true));
        assertVersionSerializable(response);
    }

    /**
     * Assert that an index creation was fully acknowledged, meaning that both the index creation cluster
     * state update was successful and that the requisite number of shard copies were started before returning.
     */
    public static void assertAcked(CreateIndexResponse response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), equalTo(true));
        assertVersionSerializable(response);
        assertTrue(response.getClass().getSimpleName() + " failed - index creation acked but not all shards were started",
            response.isShardsAcked());
    }

    /**
     * Executes the request and fails if the request has not been blocked.
     *
     * @param builder the request builder
     */
    public static void assertBlocked(ActionRequestBuilder builder) {
        assertBlocked(builder, null);
    }

    /**
     * Checks that all shard requests of a replicated broadcast request failed due to a cluster block
     *
     * @param replicatedBroadcastResponse the response that should only contain failed shard responses
     *
     * */
    public static void assertBlocked(BroadcastResponse replicatedBroadcastResponse) {
        assertThat("all shard requests should have failed", replicatedBroadcastResponse.getFailedShards(), Matchers.equalTo(replicatedBroadcastResponse.getTotalShards()));
        for (ShardOperationFailedException exception : replicatedBroadcastResponse.getShardFailures()) {
            ClusterBlockException clusterBlockException = (ClusterBlockException) ExceptionsHelper.unwrap(exception.getCause(), ClusterBlockException.class);
            assertNotNull("expected the cause of failure to be a ClusterBlockException but got " + exception.getCause().getMessage(), clusterBlockException);
            assertThat(clusterBlockException.blocks().size(), greaterThan(0));
            assertThat(clusterBlockException.status(), CoreMatchers.equalTo(RestStatus.FORBIDDEN));
        }
    }

    /**
     * Executes the request and fails if the request has not been blocked by a specific {@link ClusterBlock}.
     *
     * @param builder the request builder
     * @param expectedBlock the expected block
     */
    public static void assertBlocked(ActionRequestBuilder builder, ClusterBlock expectedBlock) {
        try {
            builder.get();
            fail("Request executed with success but a ClusterBlockException was expected");
        } catch (ClusterBlockException e) {
            assertThat(e.blocks().size(), greaterThan(0));
            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));

            if (expectedBlock != null) {
                boolean found = false;
                for (ClusterBlock clusterBlock : e.blocks()) {
                    if (clusterBlock.id() == expectedBlock.id()) {
                        found = true;
                        break;
                    }
                }
                assertThat("Request should have been blocked by [" + expectedBlock + "] instead of " + e.blocks(), found, equalTo(true));
            }
        }
    }

    public static String formatShardStatus(BroadcastResponse response) {
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

    public static void assertNoSearchHits(SearchResponse searchResponse) {
        assertEquals(0, searchResponse.getHits().getHits().length);
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        String shardStatus = formatShardStatus(searchResponse);

        Set<String> idsSet = new HashSet<>(Arrays.asList(ids));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat("id [" + hit.getId() + "] was found in search results but wasn't expected (type [" + hit.getType() + "], index [" + hit.index() + "])"
                            + shardStatus, idsSet.remove(hit.getId()),
                    equalTo(true));
        }
        assertThat("Some expected ids were not found in search results: " + Arrays.toString(idsSet.toArray(new String[idsSet.size()])) + "."
                + shardStatus, idsSet.size(), equalTo(0));
        assertVersionSerializable(searchResponse);
    }

    public static void assertSortValues(SearchResponse searchResponse, Object[]... sortValues) {
        assertSearchResponse(searchResponse);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals(sortValues.length, hits.length);
        for (int i = 0; i < sortValues.length; ++i) {
            final Object[] hitsSortValues = hits[i].getSortValues();
            assertArrayEquals("Offset " + Integer.toString(i) + ", id " + hits[i].getId(), sortValues[i], hitsSortValues);
        }
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

    public static void assertHitCount(SearchResponse countResponse, long expectedHitCount) {
        if (countResponse.getHits().totalHits() != expectedHitCount) {
            fail("Count is " + countResponse.getHits().totalHits() + " but " + expectedHitCount + " was expected. " + formatShardStatus(countResponse));
        }
        assertVersionSerializable(countResponse);
    }

    public static void assertExists(GetResponse response) {
        String message = String.format(Locale.ROOT, "Expected %s/%s/%s to exist, but does not", response.getIndex(), response.getType(), response.getId());
        assertThat(message, response.isExists(), is(true));
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

    public static void assertFourthHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 4, matcher);
    }

    public static void assertFifthHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 5, matcher);
    }

    public static void assertSearchHit(SearchResponse searchResponse, int number, Matcher<SearchHit> matcher) {
        assertThat(number, greaterThan(0));
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

    public static void assertFailures(SearchResponse searchResponse) {
        assertThat("Expected at least one shard failure, got none",
                searchResponse.getShardFailures().length, greaterThan(0));
        assertVersionSerializable(searchResponse);
    }

    public static void assertNoFailures(BulkResponse response) {
        assertThat("Unexpected ShardFailures: " + response.buildFailureMessage(),
                response.hasFailures(), is(false));
        assertVersionSerializable(response);
    }

    public static void assertFailures(SearchRequestBuilder searchRequestBuilder, RestStatus restStatus, Matcher<String> reasonMatcher) {
        //when the number for shards is randomized and we expect failures
        //we can either run into partial or total failures depending on the current number of shards
        try {
            SearchResponse searchResponse = searchRequestBuilder.get();
            assertThat("Expected shard failures, got none", searchResponse.getShardFailures().length, greaterThan(0));
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                assertThat(shardSearchFailure.status(), equalTo(restStatus));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
            assertVersionSerializable(searchResponse);
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(restStatus));
            assertThat(e.toString(), reasonMatcher);
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.status(), equalTo(restStatus));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
        } catch (Exception e) {
            fail("SearchPhaseExecutionException expected but got " + e.getClass());
        }
    }

    public static void assertNoFailures(BroadcastResponse response) {
        assertThat("Unexpected ShardFailures: " + Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));
        assertVersionSerializable(response);
    }

    public static void assertAllSuccessful(BroadcastResponse response) {
        assertNoFailures(response);
        assertThat("Expected all shards successful but got successful [" + response.getSuccessfulShards() + "] total [" + response.getTotalShards() + "]",
                response.getTotalShards(), equalTo(response.getSuccessfulShards()));
        assertVersionSerializable(response);
    }

    public static void assertAllSuccessful(SearchResponse response) {
        assertNoFailures(response);
        assertThat("Expected all shards successful but got successful [" + response.getSuccessfulShards() + "] total [" + response.getTotalShards() + "]",
                response.getTotalShards(), equalTo(response.getSuccessfulShards()));
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
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), equalTo(size));
        assertVersionSerializable(searchSuggest);
    }

    public static void assertSuggestionPhraseCollateMatchExists(Suggest searchSuggest, String key, int numberOfPhraseExists) {
        int counter = 0;
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));

        for (Suggest.Suggestion.Entry.Option option : searchSuggest.getSuggestion(key).getEntries().get(0).getOptions()) {
            if (option.collateMatch()) {
                counter++;
            }
        }

        assertThat(counter, equalTo(numberOfPhraseExists));
    }

    public static void assertSuggestion(Suggest searchSuggest, int entry, int ord, String key, String text) {
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), greaterThan(ord));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().get(ord).getText().string(), equalTo(text));
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

    /**
     * Assert that an index template is missing
     */
    public static void assertIndexTemplateMissing(GetIndexTemplatesResponse templatesResponse, String name) {
        List<String> templateNames = new ArrayList<>();
        for (IndexTemplateMetaData indexTemplateMetaData : templatesResponse.getIndexTemplates()) {
            templateNames.add(indexTemplateMetaData.name());
        }
        assertThat(templateNames, not(hasItem(name)));
    }

    /**
     * Assert that an index template exists
     */
    public static void assertIndexTemplateExists(GetIndexTemplatesResponse templatesResponse, String name) {
        List<String> templateNames = new ArrayList<>();
        for (IndexTemplateMetaData indexTemplateMetaData : templatesResponse.getIndexTemplates()) {
            templateNames.add(indexTemplateMetaData.name());
        }
        assertThat(templateNames, hasItem(name));
    }

    /**
     * Assert that aliases are missing
     */
    public static void assertAliasesMissing(AliasesExistResponse aliasesExistResponse) {
        assertFalse("Aliases shouldn't exist", aliasesExistResponse.exists());
    }

    /**
     * Assert that aliases exist
     */
    public static void assertAliasesExist(AliasesExistResponse aliasesExistResponse) {
        assertTrue("Aliases should exist", aliasesExistResponse.exists());
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
        assertThat(q.clauses().size(), greaterThan(i));
        assertThat(q.clauses().get(i).getQuery(), instanceOf(subqueryType));
        return subqueryType.cast(q.clauses().get(i).getQuery());
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type
     */
    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, Class<E> exceptionClass) {
        assertThrows(builder.execute(), exceptionClass);
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type, with a given {@link org.elasticsearch.rest.RestStatus}
     */
    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, Class<E> exceptionClass, RestStatus status) {
        assertThrows(builder.execute(), exceptionClass, status);
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type
     *
     * @param extraInfo extra information to add to the failure message
     */
    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, Class<E> exceptionClass, String extraInfo) {
        assertThrows(builder.execute(), exceptionClass, extraInfo);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass) {
        assertThrows(future, exceptionClass, null, null);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type, with a given {@link org.elasticsearch.rest.RestStatus}
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass, RestStatus status) {
        assertThrows(future, exceptionClass, status, null);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type
     *
     * @param extraInfo extra information to add to the failure message
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass, String extraInfo) {
        assertThrows(future, exceptionClass, null, extraInfo);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type, optionally checking the exception's rest status
     *
     * @param exceptionClass expected exception class
     * @param status         {@link org.elasticsearch.rest.RestStatus} to check for. Can be null to disable the check
     * @param extraInfo      extra information to add to the failure message. Can be null.
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass, @Nullable RestStatus status, @Nullable String extraInfo) {
        boolean fail = false;
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + exceptionClass + " exception to be thrown";

        if (status != null) {
            extraInfo += " with status [" + status + "]";
        }

        try {
            future.actionGet();
            fail = true;

        } catch (ElasticsearchException esException) {
            assertThat(extraInfo, esException.unwrapCause(), instanceOf(exceptionClass));
            if (status != null) {
                assertThat(extraInfo, ExceptionsHelper.status(esException), equalTo(status));
            }
        } catch (Exception e) {
            assertThat(extraInfo, e, instanceOf(exceptionClass));
            if (status != null) {
                assertThat(extraInfo, ExceptionsHelper.status(e), equalTo(status));
            }
        }
        // has to be outside catch clause to get a proper message
        if (fail) {
            throw new AssertionError(extraInfo);
        }
    }

    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, RestStatus status) {
        assertThrows(builder.execute(), status);
    }

    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, RestStatus status, String extraInfo) {
        assertThrows(builder.execute(), status, extraInfo);
    }

    public static <E extends Throwable> void assertThrows(ActionFuture future, RestStatus status) {
        assertThrows(future, status, null);
    }

    public static void assertThrows(ActionFuture future, RestStatus status, String extraInfo) {
        boolean fail = false;
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + status + " status exception to be thrown";

        try {
            future.actionGet();
            fail = true;
        } catch (Exception e) {
            assertThat(extraInfo, ExceptionsHelper.status(e), equalTo(status));
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
        assertTrue(Version.CURRENT.after(VersionUtils.getPreviousVersion()));
        assertVersionSerializable(randomVersion(random()), streamable);
    }

    public static void assertVersionSerializable(Version version, Streamable streamable) {
        /*
         * If possible we fetch the NamedWriteableRegistry from the test cluster. That is the only way to make sure that we properly handle
         * when plugins register names. If not possible we'll try and set up a registry based on whatever SearchModule registers. But that
         * is a hack at best - it only covers some things. If you end up with errors below and get to this comment I'm sorry. Please find
         * a way that sucks less.
         */
        NamedWriteableRegistry registry;
        if (ESIntegTestCase.isInternalCluster()) {
            registry = ESIntegTestCase.internalCluster().getInstance(NamedWriteableRegistry.class);
        } else {
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
            registry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        }
        assertVersionSerializable(version, streamable, registry);
    }

    public static void assertVersionSerializable(Version version, Streamable streamable, NamedWriteableRegistry namedWriteableRegistry) {
        try {
            Streamable newInstance = tryCreateNewInstance(streamable);
            if (newInstance == null) {
                return; // can't create a new instance - we never modify a
                // streamable that comes in.
            }
            if (streamable instanceof ActionRequest) {
                ((ActionRequest<?>) streamable).validate();
            }
            BytesReference orig = serialize(version, streamable);
            StreamInput input = orig.streamInput();
            if (namedWriteableRegistry != null) {
                input = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry);
            }
            input.setVersion(version);
            newInstance.readFrom(input);
            assertThat("Stream should be fully read with version [" + version + "] for streamable [" + streamable + "]", input.available(),
                    equalTo(0));
            BytesReference newBytes = serialize(version, streamable);
            if (false == orig.equals(newBytes)) {
                // The bytes are different. That is a failure. Lets try to throw a useful exception for debugging.
                String message = "Serialization failed with version [" + version + "] bytes should be equal for streamable [" + streamable
                        + "]";
                // If the bytes are different then comparing BytesRef's toStrings will show you *where* they are different
                assertEquals(message, orig.toBytesRef().toString(), newBytes.toBytesRef().toString());
                // They bytes aren't different. Very very weird.
                fail(message);
            }
        } catch (Exception ex) {
            throw new RuntimeException("failed to check serialization - version [" + version + "] for streamable [" + streamable + "]", ex);
        }

    }

    public static void assertVersionSerializable(Version version, final Exception e) {
        ElasticsearchAssertions.assertVersionSerializable(version, new ExceptionWrapper(e));
    }

    public static final class ExceptionWrapper implements Streamable {

        private Exception exception;

        public ExceptionWrapper(Exception e) {
            exception = e;
        }

        public ExceptionWrapper() {
            exception = null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            exception = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeException(exception);
        }

    }


    private static Streamable tryCreateNewInstance(Streamable streamable) throws NoSuchMethodException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        try {
            Class<? extends Streamable> clazz = streamable.getClass();
            Constructor<? extends Streamable> constructor = clazz.getConstructor();
            assertThat(constructor, Matchers.notNullValue());
            Streamable newInstance = constructor.newInstance();
            return newInstance;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Applies basic assertions on the SearchResponse. This method checks if all shards were successful, if
     * any of the shards threw an exception and if the response is serializable.
     */
    public static SearchResponse assertSearchResponse(SearchRequestBuilder request) {
        return assertSearchResponse(request.get());
    }

    /**
     * Applies basic assertions on the SearchResponse. This method checks if all shards were successful, if
     * any of the shards threw an exception and if the response is serializable.
     */
    public static SearchResponse assertSearchResponse(SearchResponse response) {
        assertNoFailures(response);
        return response;
    }

    /**
     * Check if a file exists
     */
    public static void assertFileExists(Path file) {
        assertThat("file/dir [" + file + "] should exist.", Files.exists(file), is(true));
    }

    /**
     * Check if a file does not exist
     */
    public static void assertFileNotExists(Path file) {
        assertThat("file/dir [" + file + "] should not exist.", Files.exists(file), is(false));
    }

    /**
     * Check if a directory exists
     */
    public static void assertDirectoryExists(Path dir) {
        assertFileExists(dir);
        assertThat("file [" + dir + "] should be a directory.", Files.isDirectory(dir), is(true));
    }
}
