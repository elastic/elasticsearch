/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.CheckLicenseCssAction;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.CcsLicenseChecker;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCheckLicenseCcs extends TransportMasterNodeAction<CheckLicenseCssAction.Request, CheckLicenseCssAction.Response> {

    private final Auditor auditor;
    private final Client client;

    @Inject
    public TransportCheckLicenseCcs(Settings settings, TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, ActionFilters actionFilters, Client client,
                                    IndexNameExpressionResolver indexNameExpressionResolver, Auditor auditor) {
        super(settings, CheckLicenseCssAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, CheckLicenseCssAction.Request::new);
        this.auditor = auditor;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CheckLicenseCssAction.Response newResponse() {
        return new CheckLicenseCssAction.Response();
    }

    @Override
    protected void masterOperation(CheckLicenseCssAction.Request request, ClusterState state, ActionListener<CheckLicenseCssAction.Response> listener) throws Exception {
        checkoutCssLic(activeDatafeeds(state));
    }

    @Override
    protected ClusterBlockException checkBlock(CheckLicenseCssAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    private List<DatafeedConfig> activeDatafeeds(ClusterState state) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(state);
        PersistentTasksCustomMetaData persistentTasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        return mlMetadata.getDatafeeds().values().stream()
                .filter(config -> MlMetadata.getDatafeedTask(config.getId(), persistentTasks) != null)
                .collect(Collectors.toList());
    }

    private void checkoutCssLic(List<DatafeedConfig> activeDatafeeds) {
        for (DatafeedConfig datafeed: activeDatafeeds) {
            List<String> remoteClusterNames = CcsLicenseChecker.remoteClusterNames(datafeed.getIndices());
            if (remoteClusterNames.isEmpty() == false) {
                CcsLicenseChecker remoteLicenseChecker = new CcsLicenseChecker(client);
                remoteLicenseChecker.checkRemoteClusterLicenses(remoteClusterNames,
                        ActionListener.wrap(response -> checkLicense(response, datafeed.getJobId(), datafeed.getId()),
                                e -> logger.error("Error checking remote datafeed cluster licenses", e)));
            }
        }
    }

    private void checkLicense(CcsLicenseChecker.LicenseViolation licenseCheck, String jobId, String datafeedId) {
        if (licenseCheck.isViolated()) {
            String message = "[" + jobId + "] Stoppping datafeed and closing job because Machine Learning is not licenced "
                    + "on the remote cluster [" + licenseCheck.get().getClusterName() + "] used in cross cluster search. "
                    + CcsLicenseChecker.buildErrorMessage(licenseCheck.get());
            logger.info(message);
            auditor.warning(jobId, message);

            closeJob(jobId, datafeedId);
        }
    }

    private void closeJob(String jobId, String datafeedId) {
        executeAsyncWithOrigin(client, ML_ORIGIN, StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId),
                ActionListener.wrap(
                        response -> {
                            executeAsyncWithOrigin(client, ML_ORIGIN, CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId),
                                    ActionListener.wrap(
                                            closeJobResponse -> {},
                                            e -> logger.error(
                                                    new ParameterizedMessage("[{}] An error occurred closing job", jobId), e)));
                        },
                        e -> logger.error(
                                new ParameterizedMessage("[{}] An error occurred stopping datafeed [{}]", jobId, datafeedId), e)));
    }
}
