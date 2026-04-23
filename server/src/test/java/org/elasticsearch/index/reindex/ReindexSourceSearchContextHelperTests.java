/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchContextMissingNodesException;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertSame;

public class ReindexSourceSearchContextHelperTests extends ESTestCase {

    /** A null failure is not classified as a reindex source context loss. */
    public void testIsReindexSourceContextFailureReturnsFalseForNull() {
        assertFalse(ReindexSourceSearchContextHelper.isReindexSourceContextFailure(null));
    }

    /** Direct {@link SearchContextMissingException} (missing scroll or PIT id) is a reindex source context failure. */
    public void testIsReindexSourceContextFailureReturnsTrueForSearchContextMissingException() {
        var id = new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), null);
        assertTrue(ReindexSourceSearchContextHelper.isReindexSourceContextFailure(new SearchContextMissingException(id)));
    }

    /**
     * {@link SearchContextMissingNodesException} covers PIT when required nodes left the cluster; it counts as
     * a reindex source context failure.
     */
    public void testIsReindexSourceContextFailureReturnsTrueForSearchContextMissingNodesException() {
        assertTrue(
            ReindexSourceSearchContextHelper.isReindexSourceContextFailure(
                new SearchContextMissingNodesException(
                    randomFrom(SearchContextMissingNodesException.ContextType.values()),
                    Set.of(randomAlphaOfLength(8))
                )
            )
        );
    }

    /** A {@link ReindexSourceSearchContextLostException} is obviously always considered a reindex source context failure. */
    public void testIsReindexSourceContextFailureReturnsTrueForReindexSourceSearchContextLostException() {
        var id = new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), null);
        assertTrue(
            ReindexSourceSearchContextHelper.isReindexSourceContextFailure(
                new ReindexSourceSearchContextLostException(new SearchContextMissingException(id))
            )
        );
    }

    /**
     * A wrapper exception whose cause is {@link SearchContextMissingException} is still detected (see
     * {@link org.elasticsearch.ExceptionsHelper#unwrap}).
     */
    public void testIsReindexSourceContextFailureReturnsTrueWhenCauseIsSearchContextMissingException() {
        var id = new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), null);
        var root = new SearchContextMissingException(id);
        assertTrue(ReindexSourceSearchContextHelper.isReindexSourceContextFailure(new ElasticsearchException("wrapper", root)));
    }

    /** Arbitrary errors are not reindex "lost source context" failures. */
    public void testIsReindexSourceContextFailureReturnsFalseForUnrelatedException() {
        assertFalse(ReindexSourceSearchContextHelper.isReindexSourceContextFailure(new RuntimeException("other")));
    }

    /** When detection returns false, {@code maybeWrap} must return the same throwable instance. */
    public void testMaybeWrapReturnsOriginalWhenNotContextFailure() {
        var t = new IllegalStateException("bad");
        assertSame(t, ReindexSourceSearchContextHelper.maybeWrapReindexContextFailure(t));
    }

    /** Do not double-wrap: an existing {@link ReindexSourceSearchContextLostException} is returned as-is. */
    public void testMaybeWrapReturnsOriginalWhenAlreadyReindexSourceSearchContextLostException() {
        var id = new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), null);
        var lost = new ReindexSourceSearchContextLostException(new SearchContextMissingException(id));
        assertThat(ReindexSourceSearchContextHelper.maybeWrapReindexContextFailure(lost), sameInstance(lost));
    }

    /** {@link SearchContextMissingException} is wrapped so {@link ReindexSourceSearchContextLostException#status()} can apply. */
    public void testMaybeWrapWrapsSearchContextMissingException() {
        var id = new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), null);
        var missing = new SearchContextMissingException(id);
        Throwable wrapped = ReindexSourceSearchContextHelper.maybeWrapReindexContextFailure(missing);
        assertThat(wrapped, instanceOf(ReindexSourceSearchContextLostException.class));
        assertSame(missing, wrapped.getCause());
    }

    /** Same wrapping behavior for {@link SearchContextMissingNodesException} as for missing-id failures. */
    public void testMaybeWrapWrapsSearchContextMissingNodesException() {
        var missingNodes = new SearchContextMissingNodesException(
            randomFrom(SearchContextMissingNodesException.ContextType.values()),
            Set.of(randomAlphaOfLength(8))
        );
        Throwable wrapped = ReindexSourceSearchContextHelper.maybeWrapReindexContextFailure(missingNodes);
        assertThat(wrapped, instanceOf(ReindexSourceSearchContextLostException.class));
        assertSame(missingNodes, wrapped.getCause());
    }
}
