/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Implementation of delete-by-query using scrolling and bulk.
 */
public class AsyncDeleteByQueryAction extends AbstractAsyncBulkByScrollAction<DeleteByQueryRequest> {
    public AsyncDeleteByQueryAction(BulkByScrollTask task, Logger logger, ParentTaskAssigningClient client,
                                    ThreadPool threadPool, DeleteByQueryRequest request, ScriptService scriptService,
                                    ClusterState clusterState, ActionListener<BulkByScrollResponse> listener) {
        super(task, logger, client, threadPool, request, scriptService, clusterState, listener);
    }

    @Override
    protected boolean needsSourceDocumentVersions() {
        /*
         * We always need the version of the source document so we can report a version conflict if we try to delete it and it has been
         * changed.
         */
        return true;
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
        delete.type(doc.getType());
        delete.id(doc.getId());
        delete.version(doc.getVersion());
        return wrap(delete);
    }

    /**
     * Overrides the parent's implementation is much more Update/Reindex oriented and so also copies things like timestamp/ttl which we
     * don't care for a deletion.
     */
    @Override
    protected RequestWrapper<?> copyMetadata(RequestWrapper<?> request, ScrollableHitSource.Hit doc) {
        request.setParent(doc.getParent());
        request.setRouting(doc.getRouting());
        return request;
    }

}
