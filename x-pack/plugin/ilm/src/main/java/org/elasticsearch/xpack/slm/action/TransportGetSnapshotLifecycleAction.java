/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleStats;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetSnapshotLifecycleAction extends
    TransportMasterNodeAction<GetSnapshotLifecycleAction.Request, GetSnapshotLifecycleAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetSnapshotLifecycleAction.class);

    @Inject
    public TransportGetSnapshotLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetSnapshotLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetSnapshotLifecycleAction.Request::new, indexNameExpressionResolver);
    }
    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSnapshotLifecycleAction.Response read(StreamInput in) throws IOException {
        return new GetSnapshotLifecycleAction.Response(in);
    }

    @Override
    protected void masterOperation(final Task task, final GetSnapshotLifecycleAction.Request request,
                                   final ClusterState state,
                                   final ActionListener<GetSnapshotLifecycleAction.Response> listener) {
        SnapshotLifecycleMetadata snapMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null) {
            if (request.getLifecycleIds().length == 0) {
                listener.onResponse(new GetSnapshotLifecycleAction.Response(Collections.emptyList()));
            } else {
                listener.onFailure(new ResourceNotFoundException(
                    "snapshot lifecycle policy or policies {} not found, no policies are configured",
                    Arrays.toString(request.getLifecycleIds())));
            }
        } else {
            final Map<String, SnapshotLifecyclePolicyItem.SnapshotInProgress> inProgress;
            SnapshotsInProgress sip = state.custom(SnapshotsInProgress.TYPE);
            if (sip == null) {
                inProgress = Collections.emptyMap();
            } else {
                inProgress = new HashMap<>();
                for (SnapshotsInProgress.Entry entry : sip.entries()) {
                    Map<String, Object> meta = entry.userMetadata();
                    if (meta == null ||
                        meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD) == null ||
                        (meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD) instanceof String == false)) {
                        continue;
                    }

                    String policyId = (String) meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD);
                    inProgress.put(policyId, SnapshotLifecyclePolicyItem.SnapshotInProgress.fromEntry(entry));
                }
            }

            final Set<String> ids = new HashSet<>(Arrays.asList(request.getLifecycleIds()));
            final SnapshotLifecycleStats slmStats = snapMeta.getStats();
            List<SnapshotLifecyclePolicyItem> lifecycles = snapMeta.getSnapshotConfigurations().values().stream()
                .filter(meta -> {
                    if (ids.isEmpty()) {
                        return true;
                    } else {
                        return ids.contains(meta.getPolicy().getId());
                    }
                })
                .map(policyMeta ->
                    new SnapshotLifecyclePolicyItem(policyMeta, inProgress.get(policyMeta.getPolicy().getId()),
                        slmStats.getMetrics().get(policyMeta.getPolicy().getId())))
                .collect(Collectors.toList());
            if (lifecycles.size() == 0) {
                if (request.getLifecycleIds().length == 0) {
                    listener.onResponse(new GetSnapshotLifecycleAction.Response(Collections.emptyList()));
                } else {
                    listener.onFailure(new ResourceNotFoundException("snapshot lifecycle policy or policies {} not found",
                        Arrays.toString(request.getLifecycleIds())));
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
