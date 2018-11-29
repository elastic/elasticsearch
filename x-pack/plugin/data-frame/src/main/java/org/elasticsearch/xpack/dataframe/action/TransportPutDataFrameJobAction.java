/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameJobAction.Request;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameJobAction.Response;
import org.elasticsearch.xpack.dataframe.job.DataFrameJob;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobConfig;
import org.elasticsearch.xpack.dataframe.persistence.DataframeIndex;
import org.elasticsearch.xpack.dataframe.support.JobValidator;

public class TransportPutDataFrameJobAction
        extends TransportMasterNodeAction<PutDataFrameJobAction.Request, PutDataFrameJobAction.Response> {

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportPutDataFrameJobAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, XPackLicenseState licenseState,
            PersistentTasksService persistentTasksService, Client client) {
        super(PutDataFrameJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, PutDataFrameJobAction.Request::new);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDataFrameJobAction.Response newResponse() {
        return new PutDataFrameJobAction.Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState clusterState, ActionListener<Response> listener) throws Exception {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        JobValidator jobCreator = new JobValidator(request.getConfig(), client);

        jobCreator.validate(ActionListener.wrap(validationResult -> {
            jobCreator.deduceMappings(ActionListener.wrap(mappings -> {
                DataFrameJob job = createDataFrameJob(request.getConfig(), threadPool);
                DataframeIndex.createDestinationIndex(client, job, mappings, ActionListener.wrap(createIndexResult -> {
                    startPersistentTask(job, listener, persistentTasksService);
                }, e3 -> {
                    listener.onFailure(new RuntimeException("Failed to create index", e3));
                }));
            }, e2 -> {
                listener.onFailure(new RuntimeException("Failed to deduce targe mappings", e2));
            }));
        }, e -> {
            listener.onFailure(new RuntimeException("Failed to validate", e));
        }));
    }

    private static DataFrameJob createDataFrameJob(DataFrameJobConfig config, ThreadPool threadPool) {
        return new DataFrameJob(config);
    }

    static void startPersistentTask(DataFrameJob job, ActionListener<PutDataFrameJobAction.Response> listener,
            PersistentTasksService persistentTasksService) {

        persistentTasksService.sendStartRequest(job.getConfig().getId(), DataFrameJob.NAME, job,
                ActionListener.wrap(persistentTask -> {
                    listener.onResponse(new PutDataFrameJobAction.Response(true));
                }, e -> {
                    listener.onFailure(e);
                }));
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
