/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.io.stream.MockBytesRefRecycler;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TransportSearchScrollActionTests extends ESTestCase {

    /**
     * The initial search may resolve to zero shards (empty index pattern, all shards skipped by can_match) yet still
     * encode a scroll id. That id round-trips through {@link TransportSearchHelper#parseScrollId} as a
     * {@link ParsedScrollId} with an empty context array — the input the empty-response branch is built for.
     */
    public void testParseScrollIdRoundTripsEmptyContext() {
        final String scrollId;
        try (var recycler = new MockBytesRefRecycler()) {
            scrollId = TransportSearchHelper.buildScrollId(new AtomicArray<>(0), recycler, randomBoolean());
        }
        ParsedScrollId parsed = TransportSearchHelper.parseScrollId(scrollId);
        assertThat(parsed.getContext().length, equalTo(0));
    }

    public void testEmptyResponseBuilderShape() {
        final String scrollId = randomAlphaOfLength(20);
        SearchResponse response = SearchResponse.emptyResponseBuilder().scrollId(scrollId).build();
        try {
            assertThat(response.getScrollId(), equalTo(scrollId));
            assertThat(response.getTotalShards(), equalTo(0));
            assertThat(response.getSuccessfulShards(), equalTo(0));
            assertThat(response.getSkippedShards(), equalTo(0));
            assertThat(response.getFailedShards(), equalTo(0));
            assertThat(response.getShardFailures().length, equalTo(0));
            assertThat(response.getHits().getHits().length, equalTo(0));
            assertThat(response.getTookInMillis(), equalTo(0L));
            assertThat(response.getClusters(), equalTo(SearchResponse.Clusters.EMPTY));
            TotalHits totalHits = response.getHits().getTotalHits();
            assertThat(totalHits, notNullValue());
            assertThat(totalHits.value(), equalTo(0L));
        } finally {
            response.decRef();
        }
    }

    public void testEmptyResponseBuilderDefaults() {
        SearchResponse response = SearchResponse.emptyResponseBuilder().build();
        try {
            assertThat(response.getScrollId(), nullValue());
            assertThat(response.getTookInMillis(), equalTo(0L));
            assertThat(response.getClusters(), equalTo(SearchResponse.Clusters.EMPTY));
            assertThat(response.getHits().getHits().length, equalTo(0));
        } finally {
            response.decRef();
        }
    }
}
