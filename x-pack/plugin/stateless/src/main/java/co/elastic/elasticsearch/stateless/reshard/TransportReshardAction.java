/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportReshardAction extends TransportMasterNodeAction<ReshardIndexRequest, ReshardIndexResponse> {
    public static final ActionType<ReshardIndexResponse> TYPE = new ActionType<>("indices:admin/reshard");
    private static final Logger logger = LogManager.getLogger(TransportReshardAction.class);

    private final MetadataReshardIndexService reshardIndexService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportReshardAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataReshardIndexService reshardIndexService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ReshardIndexRequest::new,
            ReshardIndexResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.reshardIndexService = reshardIndexService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(ReshardIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void masterOperation(
        Task task,
        final ReshardIndexRequest request,
        final ClusterState state,
        final ActionListener<ReshardIndexResponse> listener
    ) {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);

        /* This assert is perhaps unnecessary because we use {@link STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED}
         * option for the {@link ReshardIndexRequest}.
         * There are tests in {@link StatelessResharIT} to test that NULL/multiple indices are caught by the
         * indexNameExpressionResolver.
         */
        assert (concreteIndices != null && concreteIndices.length == 1) : "Reshard request should contain exactly one index";

        final ReshardIndexClusterStateUpdateRequest updateRequest = new ReshardIndexClusterStateUpdateRequest(
            projectResolver.getProjectId(),
            concreteIndices[0],
            request.getWaitForActiveShards()
        );

        reshardIndexService.reshardIndex(
            request.masterNodeTimeout(),
            request.ackTimeout(),
            request.ackTimeout(),
            updateRequest,
            listener.map(response -> new ReshardIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged()))
        );
    }
}
