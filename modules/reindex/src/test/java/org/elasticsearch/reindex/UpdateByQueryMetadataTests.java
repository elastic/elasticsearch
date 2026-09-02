/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.reindex.PaginatedHitSource.Hit;

public class UpdateByQueryMetadataTests extends AbstractAsyncBulkByPaginatedSearchActionMetadataTestCase<
    UpdateByQueryRequest,
    BulkByPaginatedSearchResponse> {

    public void testRoutingIsCopied() {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    @Override
    protected TestAction action() {
        return new TestAction();
    }

    @Override
    protected UpdateByQueryRequest request() {
        return new UpdateByQueryRequest();
    }

    private class TestAction extends TransportUpdateByQueryAction.AsyncIndexBySearchAction {
        TestAction() {
            super(
                UpdateByQueryMetadataTests.this.task,
                UpdateByQueryMetadataTests.this.logger,
                null,
                UpdateByQueryMetadataTests.this.threadPool,
                null,
                request(),
                listener(),
                randomPositiveTimeValue(),
                null,
                new ReindexSettings(),
                new NoopCircuitBreaker("test")
            );
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
