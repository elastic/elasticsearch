/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class TransportGetAutoFollowPatternAction
    extends TransportMasterNodeReadAction<GetAutoFollowPatternAction.Request, GetAutoFollowPatternAction.Response> {

    @Inject
    public TransportGetAutoFollowPatternAction(TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetAutoFollowPatternAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetAutoFollowPatternAction.Response read(StreamInput in) throws IOException {
        return new GetAutoFollowPatternAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, GetAutoFollowPatternAction.Request request,
                                   ClusterState state,
                                   ActionListener<GetAutoFollowPatternAction.Response> listener) throws Exception {
        Map<String, AutoFollowPattern> autoFollowPatterns = getAutoFollowPattern(state.metaData(), request.getName());
        listener.onResponse(new GetAutoFollowPatternAction.Response(autoFollowPatterns));
    }

    @Override
    protected ClusterBlockException checkBlock(GetAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    static Map<String, AutoFollowPattern> getAutoFollowPattern(MetaData metaData, String name) {
        AutoFollowMetadata autoFollowMetadata = metaData.custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            if (name == null) {
                return Collections.emptyMap();
            } else {
                throw new ResourceNotFoundException("auto-follow pattern [{}] is missing", name);
            }
        }

        if (name == null) {
            return autoFollowMetadata.getPatterns();
        }

        AutoFollowPattern autoFollowPattern = autoFollowMetadata.getPatterns().get(name);
        if (autoFollowPattern == null) {
            throw new ResourceNotFoundException("auto-follow pattern [{}] is missing", name);
        }
        return Collections.singletonMap(name, autoFollowPattern);
    }
}
