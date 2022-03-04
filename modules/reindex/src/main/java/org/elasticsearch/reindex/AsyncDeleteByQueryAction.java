/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Implementation of delete-by-query using scrolling and bulk.
 */
public class AsyncDeleteByQueryAction extends AbstractAsyncBulkByScrollAction<DeleteByQueryRequest, TransportDeleteByQueryAction> {

    public AsyncDeleteByQueryAction(
        BulkByScrollTask task,
        Logger logger,
        ParentTaskAssigningClient client,
        ThreadPool threadPool,
        DeleteByQueryRequest request,
        ScriptService scriptService,
        ActionListener<BulkByScrollResponse> listener
    ) {
        super(task, false, true, logger, client, threadPool, request, listener, scriptService, null);
    }

    @Override
    protected boolean accept(ScrollableHitSource.Hit doc) {
        // Delete-by-query does not require the source to delete a document
        // and the default implementation checks for it
        return true;
    }

    @Override
    protected RequestWrapper<DeleteRequest> buildRequest(ScrollableHitSource.Hit doc) {
        DeleteRequest delete = new DeleteRequest();
        delete.index(doc.getIndex());
        delete.id(doc.getId());
        delete.setIfSeqNo(doc.getSeqNo());
        delete.setIfPrimaryTerm(doc.getPrimaryTerm());
        return wrap(delete);
    }

    /**
     * Overrides the parent's implementation is much more Update/Reindex oriented and so also copies things like timestamp/ttl which we
     * don't care for a deletion.
     */
    @Override
    protected RequestWrapper<?> copyMetadata(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
        request.setRouting(doc.getRouting());
        return request;
    }

}
