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
import org.elasticsearch.xpack.core.enrich.GetEnrichPolicyAction;

import java.io.IOException;

public class TransportGetEnrichPolicyAction extends TransportMasterNodeAction<GetEnrichPolicyAction.Request,
    GetEnrichPolicyAction.Response> {

    @Inject
    public TransportGetEnrichPolicyAction(TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetEnrichPolicyAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetEnrichPolicyAction.Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected GetEnrichPolicyAction.Response read(StreamInput in) throws IOException {
        return new GetEnrichPolicyAction.Response();
    }

    @Override
    protected void masterOperation(GetEnrichPolicyAction.Request request,
                                   ClusterState state,
                                   ActionListener<GetEnrichPolicyAction.Response> listener) throws Exception {
        listener.onResponse(new GetEnrichPolicyAction.Response());
    }

    @Override
    protected ClusterBlockException checkBlock(GetEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
