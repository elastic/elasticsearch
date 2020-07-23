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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

public class TransportUpdateDataFrameAnalyticsAction
    extends TransportMasterNodeAction<UpdateDataFrameAnalyticsAction.Request, PutDataFrameAnalyticsAction.Response> {

    private final XPackLicenseState licenseState;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final SecurityContext securityContext;
    private final Client client;

    @Inject
    public TransportUpdateDataFrameAnalyticsAction(Settings settings, TransportService transportService, ActionFilters actionFilters,
                                                   XPackLicenseState licenseState, ThreadPool threadPool, Client client,
                                                   ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                                                   DataFrameAnalyticsConfigProvider configProvider) {
        super(UpdateDataFrameAnalyticsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            UpdateDataFrameAnalyticsAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.configProvider = configProvider;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDataFrameAnalyticsAction.Response read(StreamInput in) throws IOException {
        return new PutDataFrameAnalyticsAction.Response(in);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDataFrameAnalyticsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, UpdateDataFrameAnalyticsAction.Request request, ClusterState state,
                                   ActionListener<PutDataFrameAnalyticsAction.Response> listener) {

        Runnable doUpdate = () ->
            useSecondaryAuthIfAvailable(securityContext, () -> {
                Map<String, String> headers = threadPool.getThreadContext().getHeaders();
                configProvider.update(
                    request.getUpdate(),
                    headers,
                    state,
                    ActionListener.wrap(
                        updatedConfig -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(updatedConfig)),
                        listener::onFailure));
            });

        // Obviously if we're updating a job it's impossible that the config index has no mappings at
        // all, but if we rewrite the job config we may add new fields that require the latest mappings
        ElasticsearchMappings.addDocMappingIfMissing(
            MlConfigIndex.indexName(), MlConfigIndex::mapping, client, state,
            ActionListener.wrap(bool -> doUpdate.run(), listener::onFailure));
    }

    @Override
    protected void doExecute(Task task, UpdateDataFrameAnalyticsAction.Request request,
                             ActionListener<PutDataFrameAnalyticsAction.Response> listener) {
        if (licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
