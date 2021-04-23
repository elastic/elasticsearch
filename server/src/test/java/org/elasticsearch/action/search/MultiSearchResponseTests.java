/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchResponseTests extends AbstractXContentTestCase<MultiSearchResponse> {

    @Override
    protected MultiSearchResponse createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            // Creating a minimal response is OK, because SearchResponse self
            // is tested elsewhere.
            long tookInMillis = randomNonNegativeLong();
            int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
            int successfulShards = randomIntBetween(0, totalShards);
            int skippedShards = totalShards - successfulShards;
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
            SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, totalShards,
                    successfulShards, skippedShards, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, clusters, null);
            items[i] = new MultiSearchResponse.Item(searchResponse, null);
        }
        return new MultiSearchResponse(items, randomNonNegativeLong());
    }

    private static MultiSearchResponse createTestInstanceWithFailures() {
        int numItems = randomIntBetween(0, 128);
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            if (randomBoolean()) {
                // Creating a minimal response is OK, because SearchResponse is tested elsewhere.
                long tookInMillis = randomNonNegativeLong();
                int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
                int successfulShards = randomIntBetween(0, totalShards);
                int skippedShards = totalShards - successfulShards;
                SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
                InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
                SearchResponse searchResponse = new SearchResponse(internalSearchResponse, null, totalShards,
                        successfulShards, skippedShards, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, clusters, null);
                items[i] = new MultiSearchResponse.Item(searchResponse, null);
            } else {
                items[i] = new MultiSearchResponse.Item(null, new ElasticsearchException("an error"));
            }
        }
        return new MultiSearchResponse(items, randomNonNegativeLong());
    }

    @Override
    protected MultiSearchResponse doParseInstance(XContentParser parser) throws IOException {
        return MultiSearchResponse.fromXContext(parser);
    }

    @Override
    protected void assertEqualInstances(MultiSearchResponse expected, MultiSearchResponse actual) {
        assertThat(actual.getTook(), equalTo(expected.getTook()));
        assertThat(actual.getResponses().length, equalTo(expected.getResponses().length));
        for (int i = 0; i < expected.getResponses().length; i++) {
            MultiSearchResponse.Item expectedItem = expected.getResponses()[i];
            MultiSearchResponse.Item actualItem = actual.getResponses()[i];
            if (expectedItem.isFailure()) {
                assertThat(actualItem.getResponse(), nullValue());
                assertThat(actualItem.getFailureMessage(), containsString(expectedItem.getFailureMessage()));
            } else {
                assertThat(actualItem.getResponse().toString(), equalTo(expectedItem.getResponse().toString()));
                assertThat(actualItem.getFailure(), nullValue());
            }
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    protected Predicate<String> getRandomFieldsExcludeFilterWhenResultHasErrors() {
        return field -> field.startsWith("responses");
    }

    /**
     * Test parsing {@link MultiSearchResponse} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<MultiSearchResponse> instanceSupplier = MultiSearchResponseTests::createTestInstanceWithFailures;
        //with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        //but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields, Strings.EMPTY_ARRAY,
                getRandomFieldsExcludeFilterWhenResultHasErrors(), this::createParser, this::doParseInstance,
                this::assertEqualInstances, assertToXContentEquivalence, ToXContent.EMPTY_PARAMS);
    }

}
