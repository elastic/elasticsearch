/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction.Request;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction.Response;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJob;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfig;
import org.elasticsearch.xpack.ml.featureindexbuilder.persistence.DataframeIndex;
import org.elasticsearch.xpack.ml.featureindexbuilder.support.JobValidator;

public class TransportPutFeatureIndexBuilderJobAction
        extends TransportMasterNodeAction<PutFeatureIndexBuilderJobAction.Request, PutFeatureIndexBuilderJobAction.Response> {

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportPutFeatureIndexBuilderJobAction(Settings settings, TransportService transportService, ThreadPool threadPool,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService,
            XPackLicenseState licenseState, PersistentTasksService persistentTasksService, Client client) {
        super(PutFeatureIndexBuilderJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, PutFeatureIndexBuilderJobAction.Request::new);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFeatureIndexBuilderJobAction.Response newResponse() {
        return new PutFeatureIndexBuilderJobAction.Response();
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
                FeatureIndexBuilderJob job = createFeatureIndexBuilderJob(request.getConfig(), threadPool);
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

    private static FeatureIndexBuilderJob createFeatureIndexBuilderJob(FeatureIndexBuilderJobConfig config, ThreadPool threadPool) {
        return new FeatureIndexBuilderJob(config);
    }

    static void startPersistentTask(FeatureIndexBuilderJob job, ActionListener<PutFeatureIndexBuilderJobAction.Response> listener,
            PersistentTasksService persistentTasksService) {

        persistentTasksService.sendStartRequest(job.getConfig().getId(), FeatureIndexBuilderJob.NAME, job,
                ActionListener.wrap(persistentTask -> {
                    listener.onResponse(new PutFeatureIndexBuilderJobAction.Response(true));
                }, e -> {
                    listener.onFailure(e);
                }));
    }

    @Override
    protected ClusterBlockException checkBlock(PutFeatureIndexBuilderJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
