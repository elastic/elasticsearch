/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ScrollableHitSource.Hit;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

public class UpdateByQueryVersionTests extends AbstractAsyncBulkByScrollActionMetadataTestCase<UpdateByQueryRequest, BulkByScrollResponse> {

    UpdateByQueryRequest request;

    public void testVersion() {
        request = null;
        assertFalse(action().mainRequest.getSearchRequest().source().version());

        request = new UpdateByQueryRequest();
        request.getSearchRequest().source().version(false);
        assertFalse(action().mainRequest.getSearchRequest().source().version());

        request = new UpdateByQueryRequest();
        request.getSearchRequest().source().version(null);
        assertFalse(action().mainRequest.getSearchRequest().source().version());

        request = new UpdateByQueryRequest();
        request.getSearchRequest().source().version(true);
        assertTrue(action().mainRequest.getSearchRequest().source().version());
    }

    @Override
    protected TestAction action() {
        return new TestAction();
    }

    @Override
    protected UpdateByQueryRequest request() {
        return request != null ? request : new UpdateByQueryRequest();
    }

    private class TestAction extends TransportUpdateByQueryAction.AsyncIndexBySearchAction {
        TestAction() {
            super(
                UpdateByQueryVersionTests.this.task,
                UpdateByQueryVersionTests.this.logger,
                null,
                UpdateByQueryVersionTests.this.threadPool,
                null,
                request(),
                ClusterState.EMPTY_STATE,
                listener()
            );
        }

        @Override
        public RequestWrapper<?> copyMetadata(RequestWrapper<?> request, Hit doc) {
            return super.copyMetadata(request, doc);
        }
    }
}
