/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.hamcrest;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.IsAcknowledgedSupplier;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.apache.lucene.tests.util.LuceneTestCase.expectThrowsAnyOf;
import static org.elasticsearch.test.LambdaMatchers.transformedArrayItemsMatch;
import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticsearchAssertions {

    public static void assertAcked(AcknowledgedRequestBuilder<?, ?, ?> builder) {
        assertAcked(builder, TimeValue.timeValueSeconds(30));
    }

    public static void assertAcked(AcknowledgedRequestBuilder<?, ?, ?> builder, TimeValue timeValue) {
        assertAcked(builder.get(timeValue));
    }

    public static void assertNoTimeout(ClusterHealthRequestBuilder requestBuilder) {
        assertNoTimeout(requestBuilder.get());
    }

    public static void assertNoTimeout(ClusterHealthResponse response) {
        assertThat("ClusterHealthResponse has timed out - returned: [" + response + "]", response.isTimedOut(), is(false));
    }

    public static void assertAcked(IsAcknowledgedSupplier response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), equalTo(true));
    }

    /**
     * Assert that an index creation was fully acknowledged, meaning that both the index creation cluster
     * state update was successful and that the requisite number of shard copies were started before returning.
     */
    public static void assertAcked(CreateIndexResponse response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), is(true));
        assertThat(
            response.getClass().getSimpleName() + " failed - index creation acked but not all shards were started",
            response.isShardsAcknowledged(),
            is(true)
        );
    }

    /**
     * Executes the request and fails if the request has not been blocked.
     *
     * @param builder the request builder
     */
    public static void assertBlocked(ActionRequestBuilder<?, ?> builder) {
        assertBlocked(builder, (ClusterBlock) null);
    }

    /**
     * Checks that all shard requests of a replicated broadcast request failed due to a cluster block
     *
     * @param replicatedBroadcastResponse the response that should only contain failed shard responses
     *
     * */
    public static void assertBlocked(BaseBroadcastResponse replicatedBroadcastResponse) {
        assertThat(
            "all shard requests should have failed",
            replicatedBroadcastResponse.getFailedShards(),
            equalTo(replicatedBroadcastResponse.getTotalShards())
        );
        for (DefaultShardOperationFailedException exception : replicatedBroadcastResponse.getShardFailures()) {
            ClusterBlockException clusterBlockException = (ClusterBlockException) ExceptionsHelper.unwrap(
                exception.getCause(),
                ClusterBlockException.class
            );
            assertNotNull(
                "expected the cause of failure to be a ClusterBlockException but got " + exception.getCause().getMessage(),
                clusterBlockException
            );
            assertThat(clusterBlockException.blocks(), not(empty()));

            RestStatus status = checkRetryableBlock(clusterBlockException.blocks()) ? RestStatus.TOO_MANY_REQUESTS : RestStatus.FORBIDDEN;
            assertThat(clusterBlockException.status(), equalTo(status));
        }
    }

    /**
     * Executes the request and fails if the request has not been blocked by a specific {@link ClusterBlock}.
     *
     * @param builder the request builder
     * @param expectedBlockId the expected block id
     */
    public static void assertBlocked(final ActionRequestBuilder<?, ?> builder, @Nullable final Integer expectedBlockId) {
        var e = expectThrows(ClusterBlockException.class, builder::get);
        assertThat(e.blocks(), not(empty()));
        RestStatus status = checkRetryableBlock(e.blocks()) ? RestStatus.TOO_MANY_REQUESTS : RestStatus.FORBIDDEN;
        assertThat(e.status(), equalTo(status));

        if (expectedBlockId != null) {
            assertThat(
                "Request should have been blocked by [" + expectedBlockId + "] instead of " + e.blocks(),
                e.blocks(),
                hasItem(transformedMatch(ClusterBlock::id, equalTo(expectedBlockId)))
            );
        }
    }

    /**
     * Executes the request and fails if the request has not been blocked by a specific {@link ClusterBlock}.
     *
     * @param builder the request builder
     * @param expectedBlock the expected block
     */
    public static void assertBlocked(final ActionRequestBuilder<?, ?> builder, @Nullable final ClusterBlock expectedBlock) {
        assertBlocked(builder, expectedBlock != null ? expectedBlock.id() : null);
    }

    private static boolean checkRetryableBlock(Set<ClusterBlock> clusterBlocks) {
        // check only retryable blocks exist in the set
        return clusterBlocks.stream().allMatch(clusterBlock -> clusterBlock.id() == IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK.id());
    }

    public static String formatShardStatus(BaseBroadcastResponse response) {
        StringBuilder msg = new StringBuilder();
        msg.append(" Total shards: ")
            .append(response.getTotalShards())
            .append(" Successful shards: ")
            .append(response.getSuccessfulShards())
            .append(" & ")
            .append(response.getFailedShards())
            .append(" shard failures:");
        for (DefaultShardOperationFailedException failure : response.getShardFailures()) {
            msg.append("\n ").append(failure);
        }
        return msg.toString();
    }

    public static String formatShardStatus(SearchResponse response) {
        StringBuilder msg = new StringBuilder();
        msg.append(" Total shards: ")
            .append(response.getTotalShards())
            .append(" Successful shards: ")
            .append(response.getSuccessfulShards())
            .append(" & ")
            .append(response.getFailedShards())
            .append(" shard failures:");
        for (ShardSearchFailure failure : response.getShardFailures()) {
            msg.append("\n ").append(failure);
        }
        return msg.toString();
    }

    public static void assertNoSearchHits(SearchResponse searchResponse) {
        assertThat(searchResponse.getHits().getHits(), emptyArray());
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        assertThat(
            "Incorrect SearchHit ids. " + formatShardStatus(searchResponse),
            searchResponse.getHits(),
            transformedItemsMatch(SearchHit::getId, containsInAnyOrder(ids))
        );
    }

    public static void assertSortValues(SearchResponse searchResponse, Object[]... sortValues) {
        assertSearchResponse(searchResponse);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals(sortValues.length, hits.length);
        for (int i = 0; i < sortValues.length; ++i) {
            final Object[] hitsSortValues = hits[i].getSortValues();
            assertArrayEquals("Offset " + i + ", id " + hits[i].getId(), sortValues[i], hitsSortValues);
        }
    }

    public static void assertOrderedSearchHits(SearchResponse searchResponse, String... ids) {
        assertThat(
            "Incorrect SearchHit ids. " + formatShardStatus(searchResponse),
            searchResponse.getHits().getHits(),
            transformedArrayItemsMatch(SearchHit::getId, arrayContaining(ids))
        );
    }

    public static void assertHitCount(SearchResponse countResponse, long expectedHitCount) {
        final TotalHits totalHits = countResponse.getHits().getTotalHits();
        if (totalHits.relation != TotalHits.Relation.EQUAL_TO || totalHits.value != expectedHitCount) {
            fail("Count is " + totalHits + " but " + expectedHitCount + " was expected. " + formatShardStatus(countResponse));
        }
    }

    public static void assertExists(GetResponse response) {
        String message = String.format(Locale.ROOT, "Expected %s/%s to exist, but does not", response.getIndex(), response.getId());
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

    public static void assertSearchHit(SearchResponse searchResponse, int number, Matcher<SearchHit> matcher) {
        assertThat("SearchHit number must be greater than 0", number, greaterThan(0));
        assertThat(searchResponse.getHits().getTotalHits().value, greaterThanOrEqualTo((long) number));
        assertThat(searchResponse.getHits().getAt(number - 1), matcher);
    }

    public static void assertNoFailures(SearchResponse searchResponse) {
        assertThat(
            "Unexpected ShardFailures: " + Arrays.toString(searchResponse.getShardFailures()),
            searchResponse.getShardFailures(),
            emptyArray()
        );
    }

    public static void assertFailures(SearchResponse searchResponse) {
        assertThat("Expected at least one shard failure, got none", searchResponse.getShardFailures(), not(emptyArray()));
    }

    public static void assertNoFailures(BulkResponse response) {
        assertThat("Unexpected ShardFailures: " + response.buildFailureMessage(), response.hasFailures(), is(false));
    }

    public static void assertFailures(SearchRequestBuilder searchRequestBuilder, RestStatus restStatus, Matcher<String> reasonMatcher) {
        // when the number for shards is randomized and we expect failures
        // we can either run into partial or total failures depending on the current number of shards
        try {
            SearchResponse searchResponse = searchRequestBuilder.get();
            assertThat("Expected shard failures, got none", searchResponse.getShardFailures(), not(emptyArray()));
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                assertThat(shardSearchFailure.status(), equalTo(restStatus));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
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

    public static void assertNoFailures(BaseBroadcastResponse response) {
        if (response.getFailedShards() != 0) {
            final AssertionError assertionError = new AssertionError("[" + response.getFailedShards() + "] shard failures");

            for (DefaultShardOperationFailedException shardFailure : response.getShardFailures()) {
                assertionError.addSuppressed(new ElasticsearchException(shardFailure.toString(), shardFailure.getCause()));
            }

            throw assertionError;
        }
    }

    public static void assertAllSuccessful(BaseBroadcastResponse response) {
        assertNoFailures(response);
        assertThat("Expected all shards successful", response.getSuccessfulShards(), equalTo(response.getTotalShards()));
    }

    public static void assertAllSuccessful(SearchResponse response) {
        assertNoFailures(response);
        assertThat("Expected all shards successful", response.getSuccessfulShards(), equalTo(response.getTotalShards()));
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, Matcher<String> matcher) {
        assertHighlight(resp, hit, field, fragment, greaterThan(fragment), matcher);
    }

    public static void assertHighlight(
        SearchResponse resp,
        int hit,
        String field,
        int fragment,
        int totalFragments,
        Matcher<String> matcher
    ) {
        assertHighlight(resp, hit, field, fragment, equalTo(totalFragments), matcher);
    }

    public static void assertHighlight(SearchHit hit, String field, int fragment, Matcher<String> matcher) {
        assertHighlight(hit, field, fragment, greaterThan(fragment), matcher);
    }

    public static void assertHighlight(SearchHit hit, String field, int fragment, int totalFragments, Matcher<String> matcher) {
        assertHighlight(hit, field, fragment, equalTo(totalFragments), matcher);
    }

    private static void assertHighlight(
        SearchResponse resp,
        int hit,
        String field,
        int fragment,
        Matcher<Integer> fragmentsMatcher,
        Matcher<String> matcher
    ) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().getHits(), arrayWithSize(greaterThan(hit)));
        assertHighlight(resp.getHits().getHits()[hit], field, fragment, fragmentsMatcher, matcher);
    }

    private static void assertHighlight(
        SearchHit hit,
        String field,
        int fragment,
        Matcher<Integer> fragmentsMatcher,
        Matcher<String> matcher
    ) {
        assertThat(hit.getHighlightFields(), hasKey(field));
        assertThat(hit.getHighlightFields().get(field).fragments(), arrayWithSize(fragmentsMatcher));
        assertThat(hit.getHighlightFields().get(field).fragments()[fragment].string(), matcher);
    }

    public static void assertNotHighlighted(SearchResponse resp, int hit, String field) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().getHits(), arrayWithSize(greaterThan(hit)));
        assertThat(resp.getHits().getHits()[hit].getHighlightFields(), not(hasKey(field)));
    }

    public static void assertSuggestionSize(Suggest searchSuggest, int entry, int size, String key) {
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries(), hasSize(greaterThanOrEqualTo(entry)));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions(), hasSize(size));
    }

    public static void assertSuggestionPhraseCollateMatchExists(Suggest searchSuggest, String key, int numberOfPhraseExists) {
        int counter = 0;
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest;
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
        String msg = "Suggest result: " + searchSuggest;
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries(), hasSize(greaterThanOrEqualTo(entry)));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions(), hasSize(greaterThan(ord)));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().get(ord).getText().string(), equalTo(text));
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
        assertThat(templatesResponse.getIndexTemplates(), not(hasItem(transformedMatch(IndexTemplateMetadata::name, equalTo(name)))));
    }

    /**
     * Assert that an index template exists
     */
    public static void assertIndexTemplateExists(GetIndexTemplatesResponse templatesResponse, String name) {
        assertThat(templatesResponse.getIndexTemplates(), hasItem(transformedMatch(IndexTemplateMetadata::name, equalTo(name))));
    }

    /*
    /*
     * matchers
     */
    public static Matcher<SearchHit> hasId(final String id) {
        return transformedMatch(SearchHit::getId, equalTo(id));
    }

    public static Matcher<SearchHit> hasIndex(final String index) {
        return transformedMatch(SearchHit::getIndex, equalTo(index));
    }

    public static Matcher<SearchHit> hasScore(final float score) {
        return transformedMatch(SearchHit::getScore, equalTo(score));
    }

    public static <T extends Query> T assertBooleanSubQuery(Query query, Class<T> subqueryType, int i) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery q = (BooleanQuery) query;
        assertThat(q.clauses(), hasSize(greaterThan(i)));
        assertThat(q.clauses().get(i).getQuery(), instanceOf(subqueryType));
        return subqueryType.cast(q.clauses().get(i).getQuery());
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type
     */
    public static <E extends Throwable> void assertRequestBuilderThrows(ActionRequestBuilder<?, ?> builder, Class<E> exceptionClass) {
        assertFutureThrows(builder.execute(), exceptionClass);
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type, with a given {@link RestStatus}
     */
    public static <E extends Throwable> void assertRequestBuilderThrows(
        ActionRequestBuilder<?, ?> builder,
        Class<E> exceptionClass,
        RestStatus status
    ) {
        assertFutureThrows(builder.execute(), exceptionClass, status);
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type
     *
     * @param extraInfo extra information to add to the failure message
     */
    public static <E extends Throwable> void assertRequestBuilderThrows(
        ActionRequestBuilder<?, ?> builder,
        Class<E> exceptionClass,
        String extraInfo
    ) {
        assertFutureThrows(builder.execute(), exceptionClass, extraInfo);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type
     */
    public static <E extends Throwable> void assertFutureThrows(ActionFuture<?> future, Class<E> exceptionClass) {
        assertFutureThrows(future, exceptionClass, null, null);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type, with a given {@link RestStatus}
     */
    public static <E extends Throwable> void assertFutureThrows(ActionFuture<?> future, Class<E> exceptionClass, RestStatus status) {
        assertFutureThrows(future, exceptionClass, status, null);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type
     *
     * @param extraInfo extra information to add to the failure message
     */
    public static <E extends Throwable> void assertFutureThrows(ActionFuture<?> future, Class<E> exceptionClass, String extraInfo) {
        assertFutureThrows(future, exceptionClass, null, extraInfo);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type, optionally checking the exception's rest status
     *
     * @param exceptionClass expected exception class
     * @param status         {@link org.elasticsearch.rest.RestStatus} to check for. Can be null to disable the check
     * @param extraInfo      extra information to add to the failure message. Can be null.
     */
    public static <E extends Throwable> void assertFutureThrows(
        ActionFuture<?> future,
        Class<E> exceptionClass,
        @Nullable RestStatus status,
        @Nullable String extraInfo
    ) {
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + exceptionClass + " exception to be thrown";

        if (status != null) {
            extraInfo += " with status [" + status + "]";
        }

        Throwable expected = expectThrowsAnyOf(List.of(exceptionClass, ElasticsearchException.class), future::actionGet);
        if (expected instanceof ElasticsearchException) {
            assertThat(extraInfo, ((ElasticsearchException) expected).unwrapCause(), instanceOf(exceptionClass));
        } else {
            assertThat(extraInfo, expected, instanceOf(exceptionClass));
        }

        if (status != null) {
            assertThat(extraInfo, ExceptionsHelper.status(expected), equalTo(status));
        }
    }

    public static void assertRequestBuilderThrows(ActionRequestBuilder<?, ?> builder, RestStatus status) {
        assertFutureThrows(builder.execute(), status);
    }

    public static void assertRequestBuilderThrows(ActionRequestBuilder<?, ?> builder, RestStatus status, String extraInfo) {
        assertFutureThrows(builder.execute(), status, extraInfo);
    }

    public static void assertFutureThrows(ActionFuture<?> future, RestStatus status) {
        assertFutureThrows(future, status, null);
    }

    public static void assertFutureThrows(ActionFuture<?> future, RestStatus status, String extraInfo) {
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + status + " status exception to be thrown";

        Exception e = expectThrows(Exception.class, future::actionGet);
        assertThat(extraInfo, ExceptionsHelper.status(e), equalTo(status));
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
     * Asserts that the provided {@link BytesReference}s created through
     * {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)} hold the same content.
     * The comparison is done by parsing both into a map and comparing those two, so that keys ordering doesn't matter.
     * Also binary values (byte[]) are properly compared through arrays comparisons.
     */
    public static void assertToXContentEquivalent(BytesReference expected, BytesReference actual, XContentType xContentType)
        throws IOException {
        // we tried comparing byte per byte, but that didn't fly for a couple of reasons:
        // 1) whenever anything goes through a map while parsing, ordering is not preserved, which is perfectly ok
        // 2) Jackson SMILE parser parses floats as double, which then get printed out as double (with double precision)
        // Note that byte[] holding binary values need special treatment as they need to be properly compared item per item.
        Map<String, Object> actualMap = null;
        Map<String, Object> expectedMap = null;
        try (XContentParser actualParser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, actual.streamInput())) {
            actualMap = actualParser.map();
            try (
                XContentParser expectedParser = xContentType.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, expected.streamInput())
            ) {
                expectedMap = expectedParser.map();
                try {
                    assertMapEquals(expectedMap, actualMap);
                } catch (AssertionError error) {
                    NotEqualMessageBuilder message = new NotEqualMessageBuilder();
                    message.compareMaps(actualMap, expectedMap);
                    throw new AssertionError("Error when comparing xContent.\n" + message.toString(), error);
                }
            }
        }
    }

    /**
     * Wait for a latch to countdown and provide a useful error message if it does not
     * Often latches are called as <code>assertTrue(latch.await(1, TimeUnit.SECONDS));</code>
     * In case of a failure this will just throw an assertion error without any further message
     *
     * @param latch    The latch to wait for
     * @param timeout  The value of the timeout
     * @param unit     The unit of the timeout
     * @throws InterruptedException An exception if the waiting is interrupted
     */
    public static void awaitLatch(CountDownLatch latch, long timeout, TimeUnit unit) throws InterruptedException {
        TimeValue timeValue = new TimeValue(timeout, unit);
        String message = String.format(Locale.ROOT, "expected latch to be counted down after %s, but was not", timeValue);
        boolean isCountedDown = latch.await(timeout, unit);
        assertThat(message, isCountedDown, is(true));
    }

    /**
     * Compares two maps recursively, using arrays comparisons for byte[] through Arrays.equals(byte[], byte[])
     */
    private static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
            String expectedKey = expectedEntry.getKey();
            Object expectedValue = expectedEntry.getValue();
            if (expectedValue == null) {
                assertTrue(actual.get(expectedKey) == null && actual.containsKey(expectedKey));
            } else {
                Object actualValue = actual.get(expectedKey);
                assertObjectEquals(expectedValue, actualValue);
            }
        }
    }

    /**
     * Compares two lists recursively, but using arrays comparisons for byte[] through Arrays.equals(byte[], byte[])
     */
    private static void assertListEquals(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<Object> actualIterator = actual.iterator();
        for (Object expectedValue : expected) {
            Object actualValue = actualIterator.next();
            assertObjectEquals(expectedValue, actualValue);
        }
    }

    /**
     * Compares two objects, recursively walking eventual maps and lists encountered, and using arrays comparisons
     * for byte[] through Arrays.equals(byte[], byte[])
     */
    @SuppressWarnings("unchecked")
    private static void assertObjectEquals(Object expected, Object actual) {
        if (expected instanceof Map) {
            assertThat(actual, instanceOf(Map.class));
            assertMapEquals((Map<String, Object>) expected, (Map<String, Object>) actual);
        } else if (expected instanceof List) {
            assertListEquals((List<Object>) expected, (List<Object>) actual);
        } else if (expected instanceof byte[]) {
            // byte[] is really a special case for binary values when comparing SMILE and CBOR, arrays of other types
            // don't need to be handled. Ordinary arrays get parsed as lists.
            assertArrayEquals((byte[]) expected, (byte[]) actual);
        } else {
            assertEquals(expected, actual);
        }
    }
}
