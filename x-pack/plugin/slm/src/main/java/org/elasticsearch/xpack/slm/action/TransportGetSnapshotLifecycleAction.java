/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportGetSnapshotLifecycleAction extends TransportMasterNodeAction<
    GetSnapshotLifecycleAction.Request,
    GetSnapshotLifecycleAction.Response> {

    private final ProjectResolver projectResolver;

    @Inject
    public TransportGetSnapshotLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            GetSnapshotLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSnapshotLifecycleAction.Request::new,
            GetSnapshotLifecycleAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetSnapshotLifecycleAction.Request request,
        final ClusterState state,
        final ActionListener<GetSnapshotLifecycleAction.Response> listener
    ) {
        final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        SnapshotLifecycleMetadata snapMeta = projectMetadata.custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null) {
            if (request.getLifecycleIds().length == 0) {
                listener.onResponse(new GetSnapshotLifecycleAction.Response(Collections.emptyList()));
            } else {
                listener.onFailure(
                    new ResourceNotFoundException(
                        "snapshot lifecycle policy or policies {} not found, no policies are configured",
                        Arrays.toString(request.getLifecycleIds())
                    )
                );
            }
        } else {
            final Map<String, SnapshotLifecyclePolicyItem.SnapshotInProgress> inProgress = new HashMap<>();
            for (List<SnapshotsInProgress.Entry> entriesForRepo : SnapshotsInProgress.get(state).entriesByRepo()) {
                for (SnapshotsInProgress.Entry entry : entriesForRepo) {
                    Map<String, Object> meta = entry.userMetadata();
                    if (meta == null
                        || meta.get(SnapshotsService.POLICY_ID_METADATA_FIELD) == null
                        || (meta.get(SnapshotsService.POLICY_ID_METADATA_FIELD) instanceof String == false)) {
                        continue;
                    }

                    String policyId = (String) meta.get(SnapshotsService.POLICY_ID_METADATA_FIELD);
                    inProgress.put(policyId, SnapshotLifecyclePolicyItem.SnapshotInProgress.fromEntry(entry));
                }
            }

            final Set<String> ids = new HashSet<>(Arrays.asList(request.getLifecycleIds()));
            final SnapshotLifecycleStats slmStats = snapMeta.getStats();
            List<SnapshotLifecyclePolicyItem> lifecycles = snapMeta.getSnapshotConfigurations().values().stream().filter(meta -> {
                if (ids.isEmpty()) {
                    return true;
                } else {
                    return ids.contains(meta.getPolicy().getId());
                }
            })
                .map(
                    policyMeta -> new SnapshotLifecyclePolicyItem(
                        policyMeta,
                        inProgress.get(policyMeta.getPolicy().getId()),
                        slmStats.getMetrics().get(policyMeta.getPolicy().getId())
                    )
                )
                .toList();
            if (lifecycles.size() == 0) {
                if (request.getLifecycleIds().length == 0) {
                    listener.onResponse(new GetSnapshotLifecycleAction.Response(Collections.emptyList()));
                } else {
                    listener.onFailure(
                        new ResourceNotFoundException(
                            "snapshot lifecycle policy or policies {} not found",
                            Arrays.toString(request.getLifecycleIds())
                        )
                    );
                }
            } else {
                listener.onResponse(new GetSnapshotLifecycleAction.Response(lifecycles));
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
