/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class AsyncSearchResponseTests
        extends AbstractResponseTestCase<org.elasticsearch.xpack.core.search.action.AsyncSearchResponse, AsyncSearchResponse> {

    @Override
    protected org.elasticsearch.xpack.core.search.action.AsyncSearchResponse createServerTestInstance(XContentType xContentType) {
        boolean isPartial = randomBoolean();
        boolean isRunning = randomBoolean();
        long startTimeMillis = randomLongBetween(0, Long.MAX_VALUE);
        long expirationTimeMillis = randomLongBetween(0, Long.MAX_VALUE);
        String id = randomBoolean() ? null : randomAlphaOfLength(10);
        ElasticsearchException error = randomBoolean() ? null : new ElasticsearchException(randomAlphaOfLength(10));
        // add search response, minimal object is okay since the full randomization of parsing is tested in SearchResponseTests
        SearchResponse searchResponse = randomBoolean() ? null
                : new SearchResponse(InternalSearchResponse.empty(), randomAlphaOfLength(10), 1, 1, 0, randomIntBetween(0, 10000),
                        ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);
        org.elasticsearch.xpack.core.search.action.AsyncSearchResponse testResponse =
                new org.elasticsearch.xpack.core.search.action.AsyncSearchResponse(id, searchResponse, error, isPartial, isRunning,
                        startTimeMillis, expirationTimeMillis);
        return testResponse;
    }

    @Override
    protected AsyncSearchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return AsyncSearchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.core.search.action.AsyncSearchResponse expected, AsyncSearchResponse parsed) {
        assertNotSame(parsed, expected);
        assertEquals(expected.getId(), parsed.getId());
        assertEquals(expected.isRunning(), parsed.isRunning());
        assertEquals(expected.isPartial(), parsed.isPartial());
        assertEquals(expected.getStartTime(), parsed.getStartTime());
        assertEquals(expected.getExpirationTime(), parsed.getExpirationTime());
        // we cannot directly compare error since Exceptions are wrapped differently on parsing, but we can check original message
        if (expected.getFailure() != null) {
            assertThat(parsed.getFailure().getMessage(), containsString(expected.getFailure().getMessage()));
        } else {
            assertNull(parsed.getFailure());
        }
        // we don't need to check the complete parsed search response since this is done elsewhere
        // only spot-check some randomized properties for equality here
        if (expected.getSearchResponse() != null) {
            assertEquals(expected.getSearchResponse().getTook(), parsed.getSearchResponse().getTook());
            assertEquals(expected.getSearchResponse().getScrollId(), parsed.getSearchResponse().getScrollId());
        } else {
            assertNull(parsed.getSearchResponse());
        }
    }
}
