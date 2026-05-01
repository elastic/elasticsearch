/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link AbstractAsyncBulkByScrollAction#remapSearchFailures} and
 * {@link AbstractAsyncBulkByScrollAction#maybeWrapCatastrophicFailure}: how PIT keep-alive
 * semantics map {@link SearchContextMissingException} to {@link RestStatus} for HTTP-style failure handling.
 */
public class AbstractAsyncBulkByScrollActionPitKeepaliveHttpTests extends ESTestCase {

    /**
     * Scroll pagination keeps the original failure list and {@link RestStatus#NOT_FOUND} for
     * {@link SearchContextMissingException} even when the keep-alive deadline has passed, for backwards compatibility.
     */
    public void testRemapSearchFailuresScrollPastDeadlineLeavesListUntouched() {
        PaginatedSearchFailure paginatedSearchFailure = new PaginatedSearchFailure(missingContext());
        assertThat(paginatedSearchFailure.getStatus(), equalTo(RestStatus.NOT_FOUND));
        List<PaginatedSearchFailure> originalSearchFailures = singletonList(paginatedSearchFailure);
        List<PaginatedSearchFailure> remappedSearchFailures = AbstractAsyncBulkByScrollAction.remapSearchFailures(
            false,
            true,
            originalSearchFailures
        );
        assertThat(remappedSearchFailures, sameInstance(originalSearchFailures));
        assertThat(remappedSearchFailures.get(0).getStatus(), equalTo(RestStatus.NOT_FOUND));
    }

    /**
     * PIT pagination leaves search failures unchanged when the inferred keep-alive deadline has not yet passed,
     * so {@link SearchContextMissingException} remains {@link RestStatus#NOT_FOUND}.
     */
    public void testRemapSearchFailuresPitDeadlineNotPassedLeavesListUntouched() {
        PaginatedSearchFailure paginatedSearchFailure = new PaginatedSearchFailure(missingContext());
        List<PaginatedSearchFailure> originalSearchFailures = singletonList(paginatedSearchFailure);
        List<PaginatedSearchFailure> remappedSearchFailures = AbstractAsyncBulkByScrollAction.remapSearchFailures(
            true,
            false,
            originalSearchFailures
        );
        assertThat(remappedSearchFailures, sameInstance(originalSearchFailures));
    }

    /**
     * When using PIT and the keep-alive deadline has passed, {@link SearchContextMissingException} shard failures are
     * rewrapped with {@link RestStatus#INTERNAL_SERVER_ERROR} while preserving the same {@link Throwable} reason.
     */
    public void testRemapSearchFailuresPitPastDeadlineThrowsInternalServerError() {
        PaginatedSearchFailure paginatedSearchFailure = new PaginatedSearchFailure(missingContext());
        List<PaginatedSearchFailure> remappedSearchFailures = AbstractAsyncBulkByScrollAction.remapSearchFailures(
            true,
            true,
            singletonList(paginatedSearchFailure)
        );
        assertThat(remappedSearchFailures, hasSize(1));
        assertThat(remappedSearchFailures.get(0).getReason(), sameInstance(paginatedSearchFailure.getReason()));
        assertThat(remappedSearchFailures.get(0).getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    /**
     * When using PIT and the keep-alive deadline has passed, does not remap the list when no failure
     * unwraps to {@link SearchContextMissingException}. In this case, the original list instance is returned.
     */
    public void testRemapSearchFailuresPitPastDeadlineReturnsOriginalListForOtherFailures() {
        PaginatedSearchFailure paginatedSearchFailure = new PaginatedSearchFailure(new RuntimeException("other"));
        List<PaginatedSearchFailure> originalSearchFailures = singletonList(paginatedSearchFailure);
        List<PaginatedSearchFailure> remappedSearchFailures = AbstractAsyncBulkByScrollAction.remapSearchFailures(
            true,
            true,
            originalSearchFailures
        );
        assertThat(remappedSearchFailures, sameInstance(originalSearchFailures));
    }

    /** A null catastrophic failure stays null regardless of PIT and deadline flags. */
    public void testMaybeWrapCatastrophicFailureReturnsNullForNullFailure() {
        assertNull(AbstractAsyncBulkByScrollAction.maybeWrapCatastrophicFailure(true, true, null));
    }

    /**
     * Scroll pagination does not wrap a top-level {@link SearchContextMissingException} even after the keep-alive deadline.
     */
    public void testMaybeWrapCatastrophicFailureScrollPastDeadlinePreservesOriginalException() {
        Exception originalFailure = missingContext();
        Exception maybeWrappedFailure = AbstractAsyncBulkByScrollAction.maybeWrapCatastrophicFailure(false, true, originalFailure);
        assertThat(maybeWrappedFailure, sameInstance(originalFailure));
    }

    /**
     * PIT with the keep-alive deadline still open returns the original exception for {@link SearchContextMissingException}.
     */
    public void testMaybeWrapCatastrophicFailurePitDeadlineNotPastPreservesOriginalException() {
        Exception originalFailure = missingContext();
        Exception maybeWrappedFailure = AbstractAsyncBulkByScrollAction.maybeWrapCatastrophicFailure(true, false, originalFailure);
        assertThat(maybeWrappedFailure, sameInstance(originalFailure));
    }

    /**
     * PIT + past keep-alive deadline wraps a catastrophic {@link SearchContextMissingException} in
     * {@link ElasticsearchStatusException} with {@link RestStatus#INTERNAL_SERVER_ERROR} and the original as cause.
     */
    public void testMaybeWrapCatastrophicFailurePitPastDeadlineWrapsSearchContextMissing() {
        Exception originalFailure = missingContext();
        Exception maybeWrappedFailure = AbstractAsyncBulkByScrollAction.maybeWrapCatastrophicFailure(true, true, originalFailure);
        assertThat(maybeWrappedFailure, instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException elasticsearchStatusException = (ElasticsearchStatusException) maybeWrappedFailure;
        assertThat(elasticsearchStatusException.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(elasticsearchStatusException.getCause(), sameInstance(originalFailure));
        assertThat(elasticsearchStatusException.getMessage(), containsString("No search context found"));
    }

    /** Fixed {@link SearchContextMissingException} for assertions; details are irrelevant to HTTP remapping behavior. */
    private static SearchContextMissingException missingContext() {
        return new SearchContextMissingException(new ShardSearchContextId("session", 1L));
    }
}
