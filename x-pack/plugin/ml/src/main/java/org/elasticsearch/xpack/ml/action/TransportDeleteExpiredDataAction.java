/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ThreadedActionListener;
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
import org.elasticsearch.xpack.ml.job.retention.UnusedStateRemover;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class TransportDeleteExpiredDataAction extends HandledTransportAction<DeleteExpiredDataAction.Request,
        DeleteExpiredDataAction.Response> {

    // TODO: make configurable in the request
    static final Duration MAX_DURATION = Duration.ofHours(8);

    private final String executor;
    private final Client client;
    private final ClusterService clusterService;
    private final Clock clock;

    @Inject
    public TransportDeleteExpiredDataAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                            Client client, ClusterService clusterService) {
        this(settings, threadPool, MachineLearning.UTILITY_THREAD_POOL_NAME, transportService, actionFilters, indexNameExpressionResolver,
            client, clusterService, Clock.systemUTC());
    }

    TransportDeleteExpiredDataAction(Settings settings, ThreadPool threadPool, String executor, TransportService transportService,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client,
                                     ClusterService clusterService, Clock clock) {
        super(settings, DeleteExpiredDataAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                DeleteExpiredDataAction.Request::new, executor);
        this.executor = executor;
        this.client = ClientHelper.clientWithOrigin(client, ClientHelper.ML_ORIGIN);
        this.clusterService = clusterService;
        this.clock = clock;
    }

    @Override
    protected void doExecute(DeleteExpiredDataAction.Request request, ActionListener<DeleteExpiredDataAction.Response> listener) {
        logger.info("Deleting expired data");
        Instant timeoutTime = Instant.now(clock).plus(MAX_DURATION);
        Supplier<Boolean> isTimedOutSupplier = () -> Instant.now(clock).isAfter(timeoutTime);
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> deleteExpiredData(listener, isTimedOutSupplier));
    }

    private void deleteExpiredData(ActionListener<DeleteExpiredDataAction.Response> listener,
                                   Supplier<Boolean> isTimedOutSupplier) {
        Auditor auditor = new Auditor(client, clusterService.getNodeName());
        List<MlDataRemover> dataRemovers = Arrays.asList(
                new ExpiredResultsRemover(client, clusterService, auditor),
                new ExpiredForecastsRemover(client, threadPool),
                new ExpiredModelSnapshotsRemover(client, clusterService, threadPool),
                new UnusedStateRemover(client, clusterService)
        );
        Iterator<MlDataRemover> dataRemoversIterator = new VolatileCursorIterator<>(dataRemovers);
        deleteExpiredData(dataRemoversIterator, listener, isTimedOutSupplier, true);
    }

    void deleteExpiredData(Iterator<MlDataRemover> mlDataRemoversIterator,
                           ActionListener<DeleteExpiredDataAction.Response> listener,
                           Supplier<Boolean> isTimedOutSupplier,
                           boolean haveAllPreviousDeletionsCompleted) {
        if (haveAllPreviousDeletionsCompleted && mlDataRemoversIterator.hasNext()) {
            MlDataRemover remover = mlDataRemoversIterator.next();
            ActionListener<Boolean> nextListener = ActionListener.wrap(
                    booleanResponse -> deleteExpiredData(mlDataRemoversIterator, listener, isTimedOutSupplier, booleanResponse),
                    listener::onFailure);
            // Removing expired ML data and artifacts requires multiple operations.
            // These are queued up and executed sequentially in the action listener,
            // the chained calls must all run the ML utility thread pool NOT the thread
            // the previous action returned in which in the case of a transport_client_boss
            // thread is a disaster.
            remover.remove(new ThreadedActionListener<>(logger, threadPool, executor, nextListener, false),
                isTimedOutSupplier);
        } else {
            if (haveAllPreviousDeletionsCompleted) {
                logger.info("Completed deletion of expired ML data");
            } else {
                logger.info("Halted deletion of expired ML data until next invocation");
            }
            listener.onResponse(new DeleteExpiredDataAction.Response(haveAllPreviousDeletionsCompleted));
        }
    }
}
