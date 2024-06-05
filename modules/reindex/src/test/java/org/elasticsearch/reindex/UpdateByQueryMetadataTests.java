/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ScrollableHitSource.Hit;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

public class UpdateByQueryMetadataTests extends AbstractAsyncBulkByScrollActionMetadataTestCase<
    UpdateByQueryRequest,
    BulkByScrollResponse> {

    public void testRoutingIsCopied() {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
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
                ClusterState.EMPTY_STATE,
                listener()
            );
        }

        @Override
        public AbstractAsyncBulkByScrollAction.RequestWrapper<?> copyMetadata(
            AbstractAsyncBulkByScrollAction.RequestWrapper<?> request,
            Hit doc
        ) {
            return super.copyMetadata(request, doc);
        }
    }
}
