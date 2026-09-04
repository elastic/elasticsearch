/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.test.AbstractXContentTestCase.NUMBER_OF_TEST_RUNS;
import static org.elasticsearch.test.AbstractXContentTestCase.chunkedXContentTester;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchTemplateResponseTests extends ESTestCase {

    protected MultiSearchTemplateResponse createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        long overallTookInMillis = randomNonNegativeLong();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            // Creating a minimal response is OK, because SearchResponse self
            // is tested elsewhere.
            long tookInMillis = randomNonNegativeLong();
            int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
            int successfulShards = randomIntBetween(0, totalShards);
            int skippedShards = totalShards - successfulShards;
            SearchResponse.Clusters clusters = randomClusters();
            SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
            SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
                null,
                totalShards,
                successfulShards,
                skippedShards,
                tookInMillis,
                ShardSearchFailure.EMPTY_ARRAY,
                clusters
            );
            searchTemplateResponse.setResponse(searchResponse);
            items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
        }
        return new MultiSearchTemplateResponse(items, overallTookInMillis);
    }

    private static SearchResponse.Clusters randomClusters() {
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
    }

    private static MultiSearchTemplateResponse createTestInstanceWithFailures() {
        int numItems = randomIntBetween(0, 128);
        long overallTookInMillis = randomNonNegativeLong();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            if (randomBoolean()) {
                // Creating a minimal response is OK, because SearchResponse is tested elsewhere.
                long tookInMillis = randomNonNegativeLong();
                int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
                int successfulShards = randomIntBetween(0, totalShards);
                int skippedShards = totalShards - successfulShards;
                SearchResponse.Clusters clusters = randomClusters();
                SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
                SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
                    null,
                    totalShards,
                    successfulShards,
                    skippedShards,
                    tookInMillis,
                    ShardSearchFailure.EMPTY_ARRAY,
                    clusters
                );
                searchTemplateResponse.setResponse(searchResponse);
                items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
            } else {
                items[i] = new MultiSearchTemplateResponse.Item(null, new ElasticsearchException("an error"));
            }
        }
        return new MultiSearchTemplateResponse(items, overallTookInMillis);
    }

    protected MultiSearchTemplateResponse doParseInstance(XContentParser parser) {
        // The MultiSearchTemplateResponse is identical to the multi search response so we reuse the parsing logic in multi search response
        MultiSearchResponse mSearchResponse = SearchResponseUtils.parseMultiSearchResponse(parser);
        try {
            org.elasticsearch.action.search.MultiSearchResponse.Item[] responses = mSearchResponse.getResponses();
            MultiSearchTemplateResponse.Item[] templateResponses = new MultiSearchTemplateResponse.Item[responses.length];
            int i = 0;
            for (org.elasticsearch.action.search.MultiSearchResponse.Item item : responses) {
                SearchTemplateResponse stResponse = null;
                if (item.getResponse() != null) {
                    stResponse = new SearchTemplateResponse();
                    stResponse.setResponse(item.getResponse());
                    item.getResponse().incRef();
                }
                templateResponses[i++] = new MultiSearchTemplateResponse.Item(stResponse, item.getFailure());
            }
            return new MultiSearchTemplateResponse(templateResponses, mSearchResponse.getTook().millis());
        } finally {
            mSearchResponse.decRef();
        }
    }

    protected Predicate<String> getRandomFieldsExcludeFilterWhenResultHasErrors() {
        return field -> field.startsWith("responses");
    }

    protected void assertEqualInstances(MultiSearchTemplateResponse expectedInstance, MultiSearchTemplateResponse newInstance) {
        assertThat(newInstance.getTook(), equalTo(expectedInstance.getTook()));
        assertThat(newInstance.getResponses().length, equalTo(expectedInstance.getResponses().length));
        for (int i = 0; i < expectedInstance.getResponses().length; i++) {
            MultiSearchTemplateResponse.Item expectedItem = expectedInstance.getResponses()[i];
            MultiSearchTemplateResponse.Item actualItem = newInstance.getResponses()[i];
            if (expectedItem.isFailure()) {
                assertThat(actualItem.getResponse(), nullValue());
                assertThat(actualItem.getFailureMessage(), containsString(expectedItem.getFailureMessage()));
            } else {
                assertThat(actualItem.getResponse().toString(), equalTo(expectedItem.getResponse().toString()));
                assertThat(actualItem.getFailure(), nullValue());
            }
        }
    }

    public final void testFromXContent() throws IOException {
        chunkedXContentTester(this::createParser, t -> createTestInstance(), ToXContent.EMPTY_PARAMS, this::doParseInstance)
            .numberOfTestRuns(20)
            .supportsUnknownFields(true)
            .assertEqualsConsumer(this::assertEqualInstances)
            .dispose(MultiSearchTemplateResponse::decRef)
            .test();
    }

    /**
     * Test parsing {@link MultiSearchTemplateResponse} with inner failures as they don't support asserting on xcontent equivalence, given
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        chunkedXContentTester(this::createParser, t -> createTestInstanceWithFailures(), ToXContent.EMPTY_PARAMS, this::doParseInstance)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilterWhenResultHasErrors())
            // with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
            // but that does not bother our assertions, as we only want to test that we don't break.
            .supportsUnknownFields(true)
            // exceptions are not of the same type whenever parsed back
            .assertToXContentEquivalence(false)
            .assertEqualsConsumer(this::assertEqualInstances)
            .dispose(MultiSearchTemplateResponse::decRef)
            .test();
    }

    /**
     * With no items (or all items are failures), {@code mergeDirectoryMetrics()} must return
     * {@link DirectoryMetrics#EMPTY} — never null, and without throwing.
     */
    public void testMergeDirectoryMetricsEmpty() {
        // Zero items
        MultiSearchTemplateResponse empty = new MultiSearchTemplateResponse(new MultiSearchTemplateResponse.Item[0], 0L);
        assertThat(empty.mergeDirectoryMetrics().isEmpty(), is(true));
        empty.decRef();

        // All failures — no search response contributes
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[] {
            new MultiSearchTemplateResponse.Item(null, new ElasticsearchException("fail1")),
            new MultiSearchTemplateResponse.Item(null, new ElasticsearchException("fail2")), };
        MultiSearchTemplateResponse allFailed = new MultiSearchTemplateResponse(items, 0L);
        assertThat(allFailed.mergeDirectoryMetrics().isEmpty(), is(true));
        allFailed.decRef();
    }

    /**
     * Successful items have their {@link DirectoryMetrics} merged; the per-item bytes are summed
     * via {@link DirectoryMetrics#merge}.
     */
    public void testMergeDirectoryMetricsCorrectSum() {
        DirectoryMetrics.Builder b1 = new DirectoryMetrics.Builder();
        b1.add(StoreMetrics.NAME, new StoreMetrics(100L));
        DirectoryMetrics m1 = b1.build();

        DirectoryMetrics.Builder b2 = new DirectoryMetrics.Builder();
        b2.add(StoreMetrics.NAME, new StoreMetrics(200L));
        DirectoryMetrics m2 = b2.build();

        SearchTemplateResponse str1 = new SearchTemplateResponse();
        SearchResponse sr1 = SearchResponseUtils.response().build();
        sr1.setDirectoryMetrics(m1);
        str1.setResponse(sr1);

        SearchTemplateResponse str2 = new SearchTemplateResponse();
        SearchResponse sr2 = SearchResponseUtils.response().build();
        sr2.setDirectoryMetrics(m2);
        str2.setResponse(sr2);

        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[] {
            new MultiSearchTemplateResponse.Item(str1, null),
            new MultiSearchTemplateResponse.Item(str2, null), };
        MultiSearchTemplateResponse response = new MultiSearchTemplateResponse(items, 0L);
        DirectoryMetrics merged = response.mergeDirectoryMetrics();
        assertThat(merged.isEmpty(), is(false));
        StoreMetrics mergedStore = (StoreMetrics) merged.metrics(StoreMetrics.NAME);
        assertThat("directory metrics must sum across successful items", mergedStore.getBytesRead(), equalTo(300L));
        response.decRef();
    }

    /**
     * Failed items are skipped; only the successful items' {@link DirectoryMetrics} contribute to the merge.
     */
    public void testMergeDirectoryMetricsMixedBatch() {
        DirectoryMetrics.Builder b = new DirectoryMetrics.Builder();
        b.add(StoreMetrics.NAME, new StoreMetrics(50L));
        DirectoryMetrics m = b.build();

        SearchTemplateResponse strSuccess = new SearchTemplateResponse();
        SearchResponse srSuccess = SearchResponseUtils.response().build();
        srSuccess.setDirectoryMetrics(m);
        strSuccess.setResponse(srSuccess);

        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[] {
            new MultiSearchTemplateResponse.Item(null, new ElasticsearchException("failure")),
            new MultiSearchTemplateResponse.Item(strSuccess, null),
            new MultiSearchTemplateResponse.Item(null, new ElasticsearchException("another failure")), };
        MultiSearchTemplateResponse response = new MultiSearchTemplateResponse(items, 0L);
        DirectoryMetrics merged = response.mergeDirectoryMetrics();
        assertThat(merged.isEmpty(), is(false));
        StoreMetrics mergedStore = (StoreMetrics) merged.metrics(StoreMetrics.NAME);
        assertThat("only successful items must contribute to merged directory metrics", mergedStore.getBytesRead(), equalTo(50L));
        response.decRef();
    }
}
