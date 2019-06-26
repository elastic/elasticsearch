/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyItem;
import org.elasticsearch.xpack.core.snapshotlifecycle.action.GetSnapshotLifecycleAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetSnapshotLifecycleAction extends
    TransportMasterNodeAction<GetSnapshotLifecycleAction.Request, GetSnapshotLifecycleAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutSnapshotLifecycleAction.class);

    @Inject
    public TransportGetSnapshotLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetSnapshotLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            GetSnapshotLifecycleAction.Request::new);
    }
    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSnapshotLifecycleAction.Response newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected GetSnapshotLifecycleAction.Response read(StreamInput in) throws IOException {
        return new GetSnapshotLifecycleAction.Response(in);
    }

    @Override
    protected void masterOperation(final Task task, final GetSnapshotLifecycleAction.Request request,
                                   final ClusterState state,
                                   final ActionListener<GetSnapshotLifecycleAction.Response> listener) {
        SnapshotLifecycleMetadata snapMeta = state.metaData().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null) {
            listener.onResponse(new GetSnapshotLifecycleAction.Response(Collections.emptyList()));
        } else {
            final Set<String> ids = new HashSet<>(Arrays.asList(request.getLifecycleIds()));
            List<SnapshotLifecyclePolicyItem> lifecycles = snapMeta.getSnapshotConfigurations()
                .values()
                .stream()
                .filter(meta -> {
                    if (ids.isEmpty()) {
                        return true;
                    } else {
                        return ids.contains(meta.getPolicy().getId());
                    }
                })
                .map(SnapshotLifecyclePolicyItem::new)
                .collect(Collectors.toList());
            listener.onResponse(new GetSnapshotLifecycleAction.Response(lifecycles));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
