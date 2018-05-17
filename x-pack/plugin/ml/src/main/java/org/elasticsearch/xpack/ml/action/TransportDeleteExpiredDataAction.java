/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.retention.ExpiredForecastsRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredModelSnapshotsRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredResultsRemover;
import org.elasticsearch.xpack.ml.job.retention.MlDataRemover;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransportDeleteExpiredDataAction extends HandledTransportAction<DeleteExpiredDataAction.Request,
        DeleteExpiredDataAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportDeleteExpiredDataAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                            Client client, ClusterService clusterService) {
        super(settings, DeleteExpiredDataAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                DeleteExpiredDataAction.Request::new);
        this.client = ClientHelper.clientWithOrigin(client, ClientHelper.ML_ORIGIN);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(DeleteExpiredDataAction.Request request, ActionListener<DeleteExpiredDataAction.Response> listener) {
        logger.info("Deleting expired data");
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> deleteExpiredData(listener));
    }

    private void deleteExpiredData(ActionListener<DeleteExpiredDataAction.Response> listener) {
        Auditor auditor = new Auditor(client, clusterService.nodeName());
        List<MlDataRemover> dataRemovers = Arrays.asList(
                new ExpiredResultsRemover(client, clusterService, auditor),
                new ExpiredForecastsRemover(client),
                new ExpiredModelSnapshotsRemover(client, clusterService)
        );
        Iterator<MlDataRemover> dataRemoversIterator = new VolatileCursorIterator<>(dataRemovers);
        deleteExpiredData(dataRemoversIterator, listener);
    }

    private void deleteExpiredData(Iterator<MlDataRemover> mlDataRemoversIterator,
                                   ActionListener<DeleteExpiredDataAction.Response> listener) {
        if (mlDataRemoversIterator.hasNext()) {
            MlDataRemover remover = mlDataRemoversIterator.next();
            remover.remove(ActionListener.wrap(
                    booleanResponse -> deleteExpiredData(mlDataRemoversIterator, listener),
                    listener::onFailure));
        } else {
            logger.info("Completed deletion of expired data");
            listener.onResponse(new DeleteExpiredDataAction.Response(true));
        }
    }
}
