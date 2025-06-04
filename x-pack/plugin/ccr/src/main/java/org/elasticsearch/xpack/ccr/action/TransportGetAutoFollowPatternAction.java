/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

import java.util.Collections;
import java.util.Map;

public class TransportGetAutoFollowPatternAction extends TransportMasterNodeReadProjectAction<
    GetAutoFollowPatternAction.Request,
    GetAutoFollowPatternAction.Response> {

    @Inject
    public TransportGetAutoFollowPatternAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            GetAutoFollowPatternAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAutoFollowPatternAction.Request::new,
            projectResolver,
            GetAutoFollowPatternAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetAutoFollowPatternAction.Request request,
        ProjectState state,
        ActionListener<GetAutoFollowPatternAction.Response> listener
    ) throws Exception {
        Map<String, AutoFollowPattern> autoFollowPatterns = getAutoFollowPattern(state.metadata(), request.getName());
        listener.onResponse(new GetAutoFollowPatternAction.Response(autoFollowPatterns));
    }

    @Override
    protected ClusterBlockException checkBlock(GetAutoFollowPatternAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    static Map<String, AutoFollowPattern> getAutoFollowPattern(ProjectMetadata project, String name) {
        AutoFollowMetadata autoFollowMetadata = project.custom(AutoFollowMetadata.TYPE);
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
