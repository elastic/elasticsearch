/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleService;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TransportPutSnapshotLifecycleAction extends TransportMasterNodeAction<
    PutSnapshotLifecycleAction.Request,
    PutSnapshotLifecycleAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutSnapshotLifecycleAction.class);

    @Inject
    public TransportPutSnapshotLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutSnapshotLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutSnapshotLifecycleAction.Request::new,
            indexNameExpressionResolver,
            PutSnapshotLifecycleAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        final Task task,
        final PutSnapshotLifecycleAction.Request request,
        final ClusterState state,
        final ActionListener<PutSnapshotLifecycleAction.Response> listener
    ) {
        SnapshotLifecycleService.validateRepositoryExists(request.getLifecycle().getRepository(), state);

        SnapshotLifecycleService.validateMinimumInterval(request.getLifecycle(), state);

        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        final Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), state);
        LifecyclePolicy.validatePolicyName(request.getLifecycleId());
        clusterService.submitStateUpdateTask(
            "put-snapshot-lifecycle-" + request.getLifecycleId(),
            new AckedClusterStateUpdateTask(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    SnapshotLifecycleMetadata snapMeta = currentState.metadata().custom(SnapshotLifecycleMetadata.TYPE);

                    String id = request.getLifecycleId();
                    final SnapshotLifecycleMetadata lifecycleMetadata;
                    if (snapMeta == null) {
                        SnapshotLifecyclePolicyMetadata meta = SnapshotLifecyclePolicyMetadata.builder()
                            .setPolicy(request.getLifecycle())
                            .setHeaders(filteredHeaders)
                            .setModifiedDate(Instant.now().toEpochMilli())
                            .build();
                        lifecycleMetadata = new SnapshotLifecycleMetadata(
                            Collections.singletonMap(id, meta),
                            OperationMode.RUNNING,
                            new SnapshotLifecycleStats()
                        );
                        logger.info("adding new snapshot lifecycle [{}]", id);
                    } else {
                        Map<String, SnapshotLifecyclePolicyMetadata> snapLifecycles = new HashMap<>(snapMeta.getSnapshotConfigurations());
                        SnapshotLifecyclePolicyMetadata oldLifecycle = snapLifecycles.get(id);
                        SnapshotLifecyclePolicyMetadata newLifecycle = SnapshotLifecyclePolicyMetadata.builder(oldLifecycle)
                            .setPolicy(request.getLifecycle())
                            .setHeaders(filteredHeaders)
                            .setVersion(oldLifecycle == null ? 1L : oldLifecycle.getVersion() + 1)
                            .setModifiedDate(Instant.now().toEpochMilli())
                            .build();
                        snapLifecycles.put(id, newLifecycle);
                        lifecycleMetadata = new SnapshotLifecycleMetadata(snapLifecycles, snapMeta.getOperationMode(), snapMeta.getStats());
                        if (oldLifecycle == null) {
                            logger.info("adding new snapshot lifecycle [{}]", id);
                        } else {
                            logger.info("updating existing snapshot lifecycle [{}]", id);
                        }
                    }

                    Metadata currentMeta = currentState.metadata();
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentMeta).putCustom(SnapshotLifecycleMetadata.TYPE, lifecycleMetadata))
                        .build();
                }

                @Override
                protected PutSnapshotLifecycleAction.Response newResponse(boolean acknowledged) {
                    return new PutSnapshotLifecycleAction.Response(acknowledged);
                }
            },
            newExecutor()
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }

    @Override
    protected ClusterBlockException checkBlock(PutSnapshotLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
