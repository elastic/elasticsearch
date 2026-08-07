/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ResumeBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.ResumeBulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.ResumeReindexAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportResumeReindexAction extends AbstractResumeBulkByPaginatedSearchAction<ReindexRequest> {

    private final ReindexMetrics reindexMetrics;

    @Inject
    public TransportResumeReindexAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NodeClient nodeClient,
        ReindexMetrics reindexMetrics
    ) {
        super(
            ResumeReindexAction.NAME,
            transportService,
            actionFilters,
            in -> new ResumeBulkByPaginatedSearchRequest(in, ReindexRequest::new),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            clusterService,
            ReindexAction.INSTANCE,
            nodeClient
        );
        this.reindexMetrics = reindexMetrics;
    }

    @Override
    protected void doExecute(
        Task task,
        ResumeBulkByPaginatedSearchRequest request,
        ActionListener<ResumeBulkByPaginatedSearchResponse> listener
    ) {
        final ReindexRequest reindexRequest = (ReindexRequest) request.getDelegate();
        reindexMetrics.recordRelocation(reindexRequest.getRemoteInfo() != null, ReindexMetrics.resolveSlicingMode(reindexRequest));
        super.doExecute(task, request, listener);
    }
}
