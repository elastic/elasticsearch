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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichStore;

public class TransportPutEnrichPolicyAction extends TransportMasterNodeAction<PutEnrichPolicyAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportPutEnrichPolicyAction(TransportService transportService,
                                          ClusterService clusterService,
                                          ThreadPool threadPool,
                                          ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        super(PutEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutEnrichPolicyAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
        // TODO: Cut AcknowledgedResponse and subclasses over from streameble to writable
//        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected void masterOperation(PutEnrichPolicyAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        EnrichStore.putPolicy(request.getName(), request.getPolicy(), clusterService, e -> {
            if (e == null) {
                listener.onResponse(new AcknowledgedResponse(true));
            } else {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PutEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
