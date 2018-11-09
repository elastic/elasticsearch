/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;

import java.util.Map;

public class TransportUpdateDatafeedAction extends TransportMasterNodeAction<UpdateDatafeedAction.Request, PutDatafeedAction.Response> {

    @Inject
    public TransportUpdateDatafeedAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(UpdateDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, UpdateDatafeedAction.Request::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDatafeedAction.Response newResponse() {
        return new PutDatafeedAction.Response();
    }

    @Override
    protected void masterOperation(UpdateDatafeedAction.Request request, ClusterState state,
                                   ActionListener<PutDatafeedAction.Response> listener) {
        final Map<String, String> headers = threadPool.getThreadContext().getHeaders();

        clusterService.submitStateUpdateTask("update-datafeed-" + request.getUpdate().getId(),
                new AckedClusterStateUpdateTask<PutDatafeedAction.Response>(request, listener) {
                    private volatile DatafeedConfig updatedDatafeed;

                    @Override
                    protected PutDatafeedAction.Response newResponse(boolean acknowledged) {
                        if (acknowledged) {
                            logger.info("Updated datafeed [{}]", request.getUpdate().getId());
                        }
                        return new PutDatafeedAction.Response(updatedDatafeed);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        DatafeedUpdate update = request.getUpdate();
                        MlMetadata currentMetadata = MlMetadata.getMlMetadata(currentState);
                        PersistentTasksCustomMetaData persistentTasks =
                                currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                        MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                                .updateDatafeed(update, persistentTasks, headers).build();
                        updatedDatafeed = newMetadata.getDatafeed(update.getId());
                        return ClusterState.builder(currentState).metaData(
                                MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, newMetadata).build()).build();
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
