/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.reindex.PaginatedHitSource.Hit;

/**
 * Reindex test for routing.
 */
public class ReindexMetadataTests extends AbstractAsyncBulkByPaginatedSearchActionMetadataTestCase<
    ReindexRequest,
    BulkByPaginatedSearchResponse> {
    public void testRoutingCopiedByDefault() throws Exception {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingCopiedIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("keep");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingDiscardedIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("discard");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals(null, index.routing());
    }

    public void testRoutingSetIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("=cat");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals("cat", index.routing());
    }

    public void testRoutingSetIfWithDegenerateValue() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("==]");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals("=]", index.routing());
    }

    public void testRoutingSetFromSliceIfRequested() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        TestAction action = action();
        action.mainRequest().getDestination().routing("=cat").setRoutingFromSlice(true);
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals("cat", index.routing());
        assertTrue(index.isRoutingFromSlice());
    }

    @Override
    protected TestAction action() {
        return new TestAction();
    }

    @Override
    protected ReindexRequest request() {
        ReindexRequest request = new ReindexRequest();
        request.getDestination().index("test");
        return request;
    }

    private class TestAction extends Reindexer.AsyncIndexBySearchAction {
        TestAction() {
            super(
                ReindexMetadataTests.this.task,
                ReindexMetadataTests.this.logger,
                null,
                null,
                ReindexMetadataTests.this.threadPool,
                null,
                ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID),
                null,
                request(),
                listener(),
                randomBoolean() ? null : Version.CURRENT,
                randomPositiveTimeValue(),
                null,
                new ReindexSettings(),
                new NoopCircuitBreaker("test")
            );
        }

        public ReindexRequest mainRequest() {
            return this.mainRequest;
        }

        @Override
        public AbstractAsyncBulkByPaginatedSearchAction.RequestWrapper<?> copyMetadata(
            AbstractAsyncBulkByPaginatedSearchAction.RequestWrapper<?> request,
            Hit doc
        ) {
            return super.copyMetadata(request, doc);
        }
    }
}
