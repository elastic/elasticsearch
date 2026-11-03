/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The request that refreshes the keep-alive of fetch contexts on a data node.
 */
public class FetchContextKeepAliveRequestTests extends ESTestCase {
    private static final OriginalIndices INDICES = new OriginalIndices(
        new String[] { "logs-*", "-logs-old" },
        SearchRequest.DEFAULT_INDICES_OPTIONS
    );
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of());

    public void testRoundTrips() throws IOException {
        FetchContextKeepAliveRequest request = randomRequest();
        if (randomBoolean()) {
            request.setParentTask(randomAlphaOfLength(8), randomNonNegativeLong());
        }

        FetchContextKeepAliveRequest copy = copyWriteable(request, REGISTRY, FetchContextKeepAliveRequest::new);

        assertThat(copy, equalTo(request));
        assertThat(copy.hashCode(), equalTo(request.hashCode()));
        assertThat(copy.contextIds(), equalTo(request.contextIds()));
        assertThat(copy.keepAlive(), equalTo(request.keepAlive()));
        assertThat(copy.getParentTask(), equalTo(request.getParentTask()));
    }

    /**
     * Security resolves the index expressions of the request and replaces them, like those of the fetch request.
     */
    public void testCarriesTheIndexExpressionsOfTheQuery() {
        List<ShardSearchContextId> contextIds = randomContextIds();
        FetchContextKeepAliveRequest request = new FetchContextKeepAliveRequest(INDICES, contextIds, TimeValue.timeValueMinutes(5));

        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));
        assertThat(request.indices(), equalTo(INDICES.indices()));
        assertThat(request.indicesOptions(), equalTo(INDICES.indicesOptions()));
        assertTrue(request.includeDataStreams());

        request.indices("logs-1", "logs-2");
        assertThat(request.indices(), equalTo(new String[] { "logs-1", "logs-2" }));
        assertThat("the contexts stay", request.contextIds(), equalTo(contextIds));
    }

    /**
     * A request without index expressions would be authorized for every index.
     */
    public void testNeedsIndexExpressions() {
        OriginalIndices none = new OriginalIndices(new String[0], IndicesOptions.strictExpandOpen());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchContextKeepAliveRequest(none, randomContextIds(), TimeValue.timeValueMinutes(5))
        );
        assertThat(e.getMessage(), equalTo("a keep-alive needs the index expressions of its query"));
    }

    public void testNeedsAContext() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchContextKeepAliveRequest(INDICES, List.of(), TimeValue.timeValueMinutes(5))
        );
        assertThat(e.getMessage(), equalTo("a keep-alive needs at least one context"));
    }

    /**
     * Reader contexts keep their keep-alive in milliseconds, so a shorter one would keep a context for no time at all.
     */
    public void testNeedsAKeepAliveOfAtLeastAMillisecond() {
        TimeValue keepAlive = randomFrom(TimeValue.ZERO, TimeValue.MINUS_ONE, TimeValue.timeValueNanos(between(1, 999_999)));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchContextKeepAliveRequest(INDICES, randomContextIds(), keepAlive)
        );
        assertThat(e.getMessage(), equalTo("the keep-alive must be at least a millisecond but was [" + keepAlive + "]"));
    }

    /**
     * Only the nodes that kept contexts for the fetch phase get a keep-alive.
     */
    public void testCantBeSentToANodeWithoutTheFetchPhase() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(FetchRequest.ESQL_FETCH);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> copyWriteable(randomRequest(), REGISTRY, FetchContextKeepAliveRequest::new, older)
        );
        assertThat(e.getMessage(), containsString("can't send a fetch context keep-alive to a node on"));
    }

    private static FetchContextKeepAliveRequest randomRequest() {
        OriginalIndices indices = new OriginalIndices(
            generateRandomStringArray(5, 10, false, false),
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );
        return new FetchContextKeepAliveRequest(
            indices,
            randomContextIds(),
            TimeValue.timeValueMillis(randomLongBetween(1, TimeValue.timeValueDays(1).millis()))
        );
    }

    private static List<ShardSearchContextId> randomContextIds() {
        return randomList(
            1,
            10,
            () -> new ShardSearchContextId(randomAlphaOfLength(8), randomNonNegativeLong(), randomBoolean() ? null : randomAlphaOfLength(4))
        );
    }
}
