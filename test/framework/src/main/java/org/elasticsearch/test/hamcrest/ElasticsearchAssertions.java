/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.hamcrest;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.master.IsAcknowledgedSupplier;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
import static org.hamcrest.Matchers.containsString;
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

    public static void assertAcked(RequestBuilder<?, ? extends IsAcknowledgedSupplier> builder) {
        assertAcked(builder, TimeValue.timeValueSeconds(30));
    }

    public static void assertAcked(ActionFuture<? extends IsAcknowledgedSupplier> future) {
        assertAcked(future.actionGet());
    }

    public static void assertAcked(RequestBuilder<?, ? extends IsAcknowledgedSupplier> builder, TimeValue timeValue) {
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
    public static void assertBlocked(RequestBuilder<?, ?> builder) {
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
    public static void assertBlocked(final RequestBuilder<?, ?> builder, @Nullable final Integer expectedBlockId) {
        assertBlocked(expectedBlockId, ESTestCase.expectThrows(ClusterBlockException.class, builder));
    }

    /**
     * Checks that the given exception is a {@link ClusterBlockException}; if the given block ID is not {@code null} then the given
     * exception must match that ID.
     */
    public static void assertBlocked(@Nullable final Integer expectedBlockId, Exception exception) {
        final var e = ESTestCase.asInstanceOf(ClusterBlockException.class, exception);
        assertThat(e.blocks(), not(empty()));
        assertThat(e.status(), equalTo(checkRetryableBlock(e.blocks()) ? RestStatus.TOO_MANY_REQUESTS : RestStatus.FORBIDDEN));

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
    public static void assertBlocked(final RequestBuilder<?, ?> builder, @Nullable final ClusterBlock expectedBlock) {
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

    public static void assertNoSearchHits(SearchRequestBuilder searchRequestBuilder) {
        assertResponse(searchRequestBuilder, ElasticsearchAssertions::assertNoSearchHits);
    }

    public static void assertNoSearchHits(SearchResponse searchResponse) {
        assertThat(searchResponse.getHits().getHits(), emptyArray());
    }

    public static void assertSearchHits(SearchRequestBuilder searchRequestBuilder, String... ids) {
        assertResponse(searchRequestBuilder, res -> assertSearchHits(res, ids));
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        assertThat(
            "Incorrect SearchHit ids. " + formatShardStatus(searchResponse),
            searchResponse.getHits(),
            transformedItemsMatch(SearchHit::getId, containsInAnyOrder(ids))
        );
    }

    public static void assertSearchHitsWithoutFailures(SearchRequestBuilder requestBuilder, String... ids) {
        assertResponse(requestBuilder, res -> {
            assertNoFailures(res);
            assertHitCount(res, ids.length);
            assertSearchHits(res, ids);
        });
    }

    public static void assertSortValues(SearchRequestBuilder searchRequestBuilder, Object[]... sortValues) {
        assertNoFailuresAndResponse(searchRequestBuilder, res -> {
            SearchHit[] hits = res.getHits().getHits();
            assertEquals(sortValues.length, hits.length);
            for (int i = 0; i < sortValues.length; ++i) {
                final Object[] hitsSortValues = hits[i].getSortValues();
                assertArrayEquals("Offset " + i + ", id " + hits[i].getId(), sortValues[i], hitsSortValues);
            }
        });
    }

    public static void assertOrderedSearchHits(SearchRequestBuilder searchRequestBuilder, String... ids) {
        assertResponse(searchRequestBuilder, res -> assertOrderedSearchHits(res, ids));
    }

    public static void assertOrderedSearchHits(SearchResponse searchResponse, String... ids) {
        assertThat(
            "Incorrect SearchHit ids. " + formatShardStatus(searchResponse),
            searchResponse.getHits().getHits(),
            transformedArrayItemsMatch(SearchHit::getId, arrayContaining(ids))
        );
    }

    public static void assertHitCount(SearchRequestBuilder searchRequestBuilder, long expectedHitCount) {
        assertResponse(searchRequestBuilder, res -> assertHitCount(res, expectedHitCount));
    }

    public static void assertHitCount(ActionFuture<SearchResponse> responseFuture, long expectedHitCount) {
        try {
            assertResponse(responseFuture, res -> assertHitCount(res, expectedHitCount));
        } catch (ExecutionException | InterruptedException ex) {
            throw new AssertionError(ex);
        }
    }

    public static void assertHitCount(SearchResponse countResponse, long expectedHitCount) {
        final TotalHits totalHits = countResponse.getHits().getTotalHits();
        if (totalHits.relation() != TotalHits.Relation.EQUAL_TO || totalHits.value() != expectedHitCount) {
            fail("Count is " + totalHits + " but " + expectedHitCount + " was expected. " + formatShardStatus(countResponse));
        }
    }

    public static void assertHitCountAndNoFailures(SearchRequestBuilder searchRequestBuilder, long expectedHitCount) {
        assertNoFailuresAndResponse(searchRequestBuilder, response -> assertHitCount(response, expectedHitCount));
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
        assertThat(searchResponse.getHits().getTotalHits().value(), greaterThanOrEqualTo((long) number));
        assertThat(searchResponse.getHits().getAt(number - 1), matcher);
    }

    public static void assertNoFailures(RequestBuilder<? extends ActionRequest, SearchResponse> searchRequestBuilder) {
        assertNoFailuresAndResponse(searchRequestBuilder, r -> {});
    }

    public static void assertNoFailuresAndResponse(
        RequestBuilder<? extends ActionRequest, SearchResponse> searchRequestBuilder,
        Consumer<SearchResponse> consumer
    ) {
        assertResponse(searchRequestBuilder, res -> {
            assertNoFailures(res);
            consumer.accept(res);
        });
    }

    public static void assertNoFailuresAndResponse(ActionFuture<SearchResponse> responseFuture, Consumer<SearchResponse> consumer)
        throws ExecutionException, InterruptedException {
        var res = responseFuture.get();
        try {
            assertNoFailures(res);
            consumer.accept(res);
        } finally {
            res.decRef();
        }
    }

    public static <Q extends ActionRequest, R extends ActionResponse> void assertResponse(
        RequestBuilder<Q, R> searchRequestBuilder,
        Consumer<R> consumer
    ) {
        var res = searchRequestBuilder.get();
        try {
            consumer.accept(res);
        } finally {
            res.decRef();
        }
    }

    /**
     * A helper to enable the testing of scroll requests with ref-counting.
     *
     * @param keepAlive             The TTL for the scroll context.
     * @param searchRequestBuilder  The initial search request.
     * @param expectedTotalHitCount The number of hits that are expected to be retrieved.
     * @param responseConsumer      (respNum, response) -> {your assertions here}.
     *                              respNum starts at 1, which contains the resp from the initial request.
     */
    public static void assertScrollResponsesAndHitCount(
        Client client,
        TimeValue keepAlive,
        SearchRequestBuilder searchRequestBuilder,
        int expectedTotalHitCount,
        BiConsumer<Integer, SearchResponse> responseConsumer
    ) {
        searchRequestBuilder.setScroll(keepAlive);
        List<SearchResponse> responses = new ArrayList<>();
        var scrollResponse = searchRequestBuilder.get();
        responses.add(scrollResponse);
        int retrievedDocsCount = 0;
        try {
            assertThat(scrollResponse.getHits().getTotalHits().value(), equalTo((long) expectedTotalHitCount));
            retrievedDocsCount += scrollResponse.getHits().getHits().length;
            responseConsumer.accept(responses.size(), scrollResponse);
            while (scrollResponse.getHits().getHits().length > 0) {
                scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(keepAlive).get();
                responses.add(scrollResponse);
                assertThat(scrollResponse.getHits().getTotalHits().value(), equalTo((long) expectedTotalHitCount));
                retrievedDocsCount += scrollResponse.getHits().getHits().length;
                responseConsumer.accept(responses.size(), scrollResponse);
            }
        } finally {
            ClearScrollResponse clearResponse = client.prepareClearScroll().setScrollIds(Arrays.asList(scrollResponse.getScrollId())).get();
            responses.forEach(SearchResponse::decRef);
            assertThat(clearResponse.isSucceeded(), Matchers.equalTo(true));
        }
        assertThat(retrievedDocsCount, equalTo(expectedTotalHitCount));
    }

    public static <R extends ActionResponse> void assertResponse(ActionFuture<R> responseFuture, Consumer<R> consumer)
        throws ExecutionException, InterruptedException {
        var res = responseFuture.get();
        try {
            consumer.accept(res);
        } finally {
            res.decRef();
        }
    }

    public static void assertCheckedResponse(
        RequestBuilder<?, SearchResponse> searchRequestBuilder,
        CheckedConsumer<SearchResponse, IOException> consumer
    ) throws IOException {
        var res = searchRequestBuilder.get();
        try {
            consumer.accept(res);
        } finally {
            res.decRef();
        }
    }

    public static void assertCheckedResponse(
        ActionFuture<SearchResponse> responseFuture,
        CheckedConsumer<SearchResponse, IOException> consumer
    ) throws IOException, ExecutionException, InterruptedException {
        var res = responseFuture.get();
        try {
            consumer.accept(res);
        } finally {
            res.decRef();
        }
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
        assertFailures(searchRequestBuilder, Set.of(restStatus), reasonMatcher);
    }

    public static void assertFailures(
        SearchRequestBuilder searchRequestBuilder,
        Set<RestStatus> restStatuses,
        Matcher<String> reasonMatcher
    ) {
        // when the number for shards is randomized and we expect failures
        // we can either run into partial or total failures depending on the current number of shards
        try {
            assertResponse(searchRequestBuilder, response -> {
                assertThat("Expected shard failures, got none", response.getShardFailures(), not(emptyArray()));
                for (ShardSearchFailure shardSearchFailure : response.getShardFailures()) {
                    assertThat(restStatuses, hasItem(shardSearchFailure.status()));
                    assertThat(shardSearchFailure.reason(), reasonMatcher);
                }
            });
        } catch (SearchPhaseExecutionException e) {
            assertThat(restStatuses, hasItem(e.status()));
            assertThat(e.toString(), reasonMatcher);
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(restStatuses, hasItem(shardSearchFailure.status()));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
        } catch (Exception e) {
            fail("SearchPhaseExecutionException expected but got " + e.getClass());
        }
    }

    public static void assertFailures(SearchRequestBuilder searchRequestBuilder, RestStatus restStatus) {
        assertFailures(searchRequestBuilder, restStatus, containsString(""));
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
        SearchRequestBuilder searchRequestBuilder,
        int hit,
        String field,
        int fragment,
        int totalFragments,
        Matcher<String> matcher
    ) {
        assertResponse(searchRequestBuilder, response -> assertHighlight(response, hit, field, fragment, equalTo(totalFragments), matcher));
    }

    public static void assertHighlight(
        ActionFuture<SearchResponse> responseFuture,
        int hit,
        String field,
        int fragment,
        int totalFragments,
        Matcher<String> matcher
    ) throws ExecutionException, InterruptedException {
        assertResponse(responseFuture, response -> assertHighlight(response, hit, field, fragment, equalTo(totalFragments), matcher));
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

    public static void assertNotHighlighted(SearchRequestBuilder searchRequestBuilder, int hit, String field) {
        assertResponse(searchRequestBuilder, response -> assertNotHighlighted(response, hit, field));
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

    public static Matcher<SearchHit> hasRank(final int rank) {
        return transformedMatch(SearchHit::getRank, equalTo(rank));
    }

    public static <T extends Query> T assertBooleanSubQuery(Query query, Class<T> subqueryType, int i) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery q = (BooleanQuery) query;
        assertThat(q.clauses(), hasSize(greaterThan(i)));
        assertThat(q.clauses().get(i).query(), instanceOf(subqueryType));
        return subqueryType.cast(q.clauses().get(i).query());
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type, with a given {@link RestStatus}
     */
    public static <E extends Throwable> void assertRequestBuilderThrows(
        RequestBuilder<?, ?> builder,
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
        RequestBuilder<?, ?> builder,
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

    public static void assertRequestBuilderThrows(RequestBuilder<?, ?> builder, RestStatus status) {
        assertFutureThrows(builder.execute(), status);
    }

    public static void assertRequestBuilderThrows(RequestBuilder<?, ?> builder, RestStatus status, String extraInfo) {
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
        try (
            XContentParser actualParser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, actual, xContentType)
        ) {
            actualMap = actualParser.map();
            try (
                XContentParser expectedParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    expected,
                    xContentType
                )
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
     * @param latch   The latch to wait for
     * @param timeout The value of the timeout
     * @param unit    The unit of the timeout
     * @throws InterruptedException An exception if the waiting is interrupted
     */
    public static void awaitLatch(CountDownLatch latch, long timeout, TimeUnit unit) throws InterruptedException {
        TimeValue timeValue = new TimeValue(timeout, unit);
        String message = String.format(Locale.ROOT, "expected latch to be counted down after %s, but was not", timeValue);
        boolean isCountedDown = latch.await(timeout, unit);
        assertThat(message, isCountedDown, is(true));
    }

    /**
     * Check the response of a client request to ensure it has a warning header that contains the provided string
     *
     * Currently, this allows a fixed 10 seconds for response to be received
     * @param client Client making the request - Required to access the response headers
     * @param requestBuilder Request to be made
     * @param toMatch String to match in the warning headers
     * @param <T> Type of the response
     * @throws InterruptedException If the request times out
     */
    public static <T extends ActionResponse> void assertWarningHeaderOnResponse(
        Client client,
        ActionRequestBuilder<?, T> requestBuilder,
        String toMatch
    ) throws InterruptedException {
        assertWarningHeaderMatchOnResponse(client, requestBuilder, hasItem(containsString(toMatch)));
    }

    /**
     * Check the response of a client request to ensure it does not have a warning header that contains the provided string
     *
     * Currently, this allows a fixed 10 seconds for response to be received
     * @param client Client making the request - Required to access the response headers
     * @param requestBuilder Request to be made
     * @param toMatch String to not match in the warning headers
     * @param <T> Type of the response
     * @throws InterruptedException If the request times out
     */
    public static <T extends ActionResponse> void assertNoWarningHeaderOnResponse(
        Client client,
        ActionRequestBuilder<?, T> requestBuilder,
        String toMatch
    ) throws InterruptedException {
        assertWarningHeaderMatchOnResponse(client, requestBuilder, not(hasItem(containsString(toMatch))));
    }

    private static <T extends ActionResponse> void assertWarningHeaderMatchOnResponse(
        Client client,
        ActionRequestBuilder<?, T> requestBuilder,
        Matcher<? super List<String>> matcher
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        requestBuilder.execute(new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                try {
                    final var warningHeaders = client.threadPool().getThreadContext().getResponseHeaders().get("Warning");
                    assertThat(warningHeaders, matcher);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    throw new AssertionError("Failed to execute request", e);
                } finally {
                    latch.countDown();
                }
            }
        });
        if (latch.await(10, TimeUnit.SECONDS) == false) {
            fail("Did not receive request response before timeout");
        }
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
