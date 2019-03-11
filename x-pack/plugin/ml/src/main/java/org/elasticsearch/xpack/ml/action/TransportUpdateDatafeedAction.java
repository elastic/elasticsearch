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
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.Collections;
import java.util.Map;

public class TransportUpdateDatafeedAction extends TransportMasterNodeAction<UpdateDatafeedAction.Request, PutDatafeedAction.Response> {

    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobConfigProvider jobConfigProvider;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportUpdateDatafeedAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client, NamedXContentRegistry xContentRegistry) {
        super(settings, UpdateDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, UpdateDatafeedAction.Request::new);

        datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
        jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        this.xContentRegistry = xContentRegistry;
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
                                   ActionListener<PutDatafeedAction.Response> listener) throws Exception {

        if (migrationEligibilityCheck.datafeedIsEligibleForMigration(request.getUpdate().getId(), state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("update datafeed", request.getUpdate().getId()));
            return;
        }

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(state);
        boolean datafeedConfigIsInClusterState = mlMetadata.getDatafeed(request.getUpdate().getId()) != null;
        if (datafeedConfigIsInClusterState) {
            updateDatafeedInClusterState(request, listener);
        } else {
            updateDatafeedInIndex(request, state, listener);
        }
    }

    private void updateDatafeedInIndex(UpdateDatafeedAction.Request request, ClusterState state,
                                       ActionListener<PutDatafeedAction.Response> listener) throws Exception {
        final Map<String, String> headers = threadPool.getThreadContext().getHeaders();

        // Check datafeed is stopped
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (MlTasks.getDatafeedTask(request.getUpdate().getId(), tasks) != null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException(
                    Messages.getMessage(Messages.DATAFEED_CANNOT_UPDATE_IN_CURRENT_STATE,
                            request.getUpdate().getId(), DatafeedState.STARTED)));
            return;
        }

        String datafeedId = request.getUpdate().getId();

        CheckedConsumer<Boolean, Exception> updateConsumer = ok -> {
            datafeedConfigProvider.updateDatefeedConfig(request.getUpdate().getId(), request.getUpdate(), headers,
                    jobConfigProvider::validateDatafeedJob, clusterService.state().nodes().getMinNodeVersion(),
                    ActionListener.wrap(
                            updatedConfig -> listener.onResponse(new PutDatafeedAction.Response(updatedConfig)),
                            listener::onFailure
                    ));
        };


        if (request.getUpdate().getJobId() != null) {
            checkJobDoesNotHaveADifferentDatafeed(request.getUpdate().getJobId(), datafeedId,
                    ActionListener.wrap(updateConsumer, listener::onFailure));
        } else {
            updateConsumer.accept(Boolean.TRUE);
        }
    }

    /*
     * This is a check against changing the datafeed's jobId and that job
     * already having a datafeed.
     * The job the updated datafeed refers to should have no datafeed or
     * if it does have a datafeed it must be the one we are updating
     */
    private void checkJobDoesNotHaveADifferentDatafeed(String jobId, String datafeedId, ActionListener<Boolean> listener) {
        datafeedConfigProvider.findDatafeedsForJobIds(Collections.singletonList(jobId), ActionListener.wrap(
                datafeedIds -> {
                    if (datafeedIds.isEmpty()) {
                        // Ok the job does not have a datafeed
                        listener.onResponse(Boolean.TRUE);
                    } else if (datafeedIds.size() == 1 && datafeedIds.contains(datafeedId)) {
                        // Ok the job has the datafeed being updated
                        listener.onResponse(Boolean.TRUE);
                    } else {
                        listener.onFailure(ExceptionsHelper.conflictStatusException("A datafeed [" + datafeedIds.iterator().next()
                                + "] already exists for job [" + jobId + "]"));
                    }
                },
                listener::onFailure
        ));
    }

    private void updateDatafeedInClusterState(UpdateDatafeedAction.Request request,
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
                                .updateDatafeed(update, persistentTasks, headers, xContentRegistry).build();
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
