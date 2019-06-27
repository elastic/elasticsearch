/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.io.IOException;

public class TransportDeleteEnrichPolicyAction extends TransportMasterNodeAction<DeleteEnrichPolicyAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportDeleteEnrichPolicyAction(TransportService transportService,
                                             ClusterService clusterService,
                                             ThreadPool threadPool,
                                             ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver) {
        super(DeleteEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeleteEnrichPolicyAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, DeleteEnrichPolicyAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        EnrichStore.deletePolicy(request.getName(), clusterService, e -> {
           if (e == null) {
               listener.onResponse(new AcknowledgedResponse(true));
           } else {
               listener.onFailure(e);
           }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
