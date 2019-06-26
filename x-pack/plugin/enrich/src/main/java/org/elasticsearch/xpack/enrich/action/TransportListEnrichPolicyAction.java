/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.io.IOException;
import java.util.Map;

public class TransportListEnrichPolicyAction
    extends TransportMasterNodeAction<ListEnrichPolicyAction.Request, ListEnrichPolicyAction.Response> {

    @Inject
    public TransportListEnrichPolicyAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ListEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ListEnrichPolicyAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ListEnrichPolicyAction.Response read(StreamInput in) throws IOException {
        return new ListEnrichPolicyAction.Response(in);
    }

    @Override
    protected ListEnrichPolicyAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected void masterOperation(ListEnrichPolicyAction.Request request, ClusterState state,
                                   ActionListener<ListEnrichPolicyAction.Response> listener) throws Exception {
        Map<String, EnrichPolicy> policies = EnrichStore.getPolicies(clusterService.state());
        listener.onResponse(new ListEnrichPolicyAction.Response(policies));
    }

    @Override
    protected ClusterBlockException checkBlock(ListEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }


}
