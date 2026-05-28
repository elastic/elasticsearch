/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.ExecutorService;

/// Transport action for batch cancellation of recoveries on a data node.
/// TODO: support cancelShardRecovery, see https://github.com/elastic/elasticsearch/pull/149553.
public class TransportCancelRecoveriesAction extends HandledTransportAction<CancelRecoveriesAction.Request, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportCancelRecoveriesAction.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final ExecutorService executor;

    @Inject
    public TransportCancelRecoveriesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        PeerRecoveryTargetService peerRecoveryTargetService
    ) {
        super(
            CancelRecoveriesAction.TYPE.name(),
            transportService,
            actionFilters,
            CancelRecoveriesAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doExecute(Task task, CancelRecoveriesAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        RecoveryClusterStateDelay.ensureClusterStateVersion(
            request.clusterStateVersion(),
            clusterService,
            executor,
            null,
            listener,
            l -> processCancellations(request, l)
        );
    }

    private void processCancellations(CancelRecoveriesAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        for (CancelRecoveriesAction.ShardRecoveryCancellation cancellation : request.cancellations()) {
            cancelShardRecovery(cancellation, request.clusterStateVersion());
        }
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }

    private void cancelShardRecovery(CancelRecoveriesAction.ShardRecoveryCancellation cancellation, long clusterStateVersion) {
        assert false : "direct cancellation is not yet supported " + cancellation;
        logger.error(
            "Direct cancellation of shard recovery is not yet supported. Shard cancellation requested {} with cluster state version {}",
            cancellation,
            clusterStateVersion
        );
    }
}
