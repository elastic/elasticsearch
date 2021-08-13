/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.index.reindex.ScrollableHitSource.Hit;
import org.elasticsearch.action.index.IndexRequest;

/**
 * Index-by-search test for ttl, timestamp, and routing.
 */
public class ReindexMetadataTests extends AbstractAsyncBulkByScrollActionMetadataTestCase<ReindexRequest, BulkByScrollResponse> {
    public void testRoutingCopiedByDefault() throws Exception {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingCopiedIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("keep");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingDiscardedIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("discard");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals(null, index.routing());
    }

    public void testRoutingSetIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("=cat");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("cat", index.routing());
    }

    public void testRoutingSetIfWithDegenerateValue() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("==]");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("=]", index.routing());
    }

    @Override
    protected TestAction action() {
        return new TestAction();
    }

    @Override
    protected ReindexRequest request() {
        return new ReindexRequest();
    }

    private class TestAction extends Reindexer.AsyncIndexBySearchAction {
        TestAction() {
            super(ReindexMetadataTests.this.task, ReindexMetadataTests.this.logger, null, null, ReindexMetadataTests.this.threadPool,
                null, null, request(), listener());
        }

        public ReindexRequest mainRequest() {
            return this.mainRequest;
        }

        @Override
        public AbstractAsyncBulkByScrollAction.RequestWrapper<?> copyMetadata(AbstractAsyncBulkByScrollAction.RequestWrapper<?> request,
                Hit doc) {
            return super.copyMetadata(request, doc);
        }
    }
}
