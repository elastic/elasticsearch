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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

import java.util.Collections;
import java.util.Map;

public class TransportGetAutoFollowPatternAction
    extends TransportMasterNodeReadAction<GetAutoFollowPatternAction.Request, GetAutoFollowPatternAction.Response> {

    @Inject
    public TransportGetAutoFollowPatternAction(Settings settings,
                                               TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetAutoFollowPatternAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetAutoFollowPatternAction.Response newResponse() {
        return new GetAutoFollowPatternAction.Response();
    }

    @Override
    protected void masterOperation(GetAutoFollowPatternAction.Request request,
                                   ClusterState state,
                                   ActionListener<GetAutoFollowPatternAction.Response> listener) throws Exception {
        Map<String, AutoFollowPattern> autoFollowPatterns = getAutoFollowPattern(state.metaData(), request.getLeaderClusterAlias());
        listener.onResponse(new GetAutoFollowPatternAction.Response(autoFollowPatterns));
    }

    @Override
    protected ClusterBlockException checkBlock(GetAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    static Map<String, AutoFollowPattern> getAutoFollowPattern(MetaData metaData, String leaderClusterAlias) {
        AutoFollowMetadata autoFollowMetadata = metaData.custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            throw new ResourceNotFoundException("no auto-follow patterns for cluster alias [{}] found", leaderClusterAlias);
        }

        if (leaderClusterAlias == null) {
            return autoFollowMetadata.getPatterns();
        }

        AutoFollowPattern autoFollowPattern = autoFollowMetadata.getPatterns().get(leaderClusterAlias);
        if (autoFollowPattern == null) {
            throw new ResourceNotFoundException("no auto-follow patterns for cluster alias [{}] found", leaderClusterAlias);
        }
        return Collections.singletonMap(leaderClusterAlias, autoFollowPattern);
    }
}
