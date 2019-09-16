/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.io.IOException;
import java.util.Map;

public class TransportUpdateDatafeedAction extends
    TransportMasterNodeAction<UpdateDatafeedAction.Request, PutDatafeedAction.Response> {

    private final Client client;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobConfigProvider jobConfigProvider;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    @Inject
    public TransportUpdateDatafeedAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client, NamedXContentRegistry xContentRegistry) {
        super(UpdateDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters, UpdateDatafeedAction.Request::new,
                indexNameExpressionResolver);

        this.client = client;
        this.datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
        this.jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDatafeedAction.Response read(StreamInput in) throws IOException {
        return new PutDatafeedAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, UpdateDatafeedAction.Request request, ClusterState state,
                                   ActionListener<PutDatafeedAction.Response> listener) throws Exception {

        if (migrationEligibilityCheck.datafeedIsEligibleForMigration(request.getUpdate().getId(), state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("update datafeed", request.getUpdate().getId()));
            return;
        }

        final Map<String, String> headers = threadPool.getThreadContext().getHeaders();

        // Check datafeed is stopped
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (MlTasks.getDatafeedTask(request.getUpdate().getId(), tasks) != null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException(
                    Messages.getMessage(Messages.DATAFEED_CANNOT_UPDATE_IN_CURRENT_STATE,
                            request.getUpdate().getId(), DatafeedState.STARTED)));
            return;
        }

        datafeedConfigProvider.updateDatefeedConfig(
            request.getUpdate().getId(),
            request.getUpdate(),
            headers,
            jobConfigProvider::validateDatafeedJob,
            ActionListener.wrap(
                updatedConfig -> listener.onResponse(new PutDatafeedAction.Response(updatedConfig)),
                listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
