/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.retention.EmptyStateIndexRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredForecastsRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredModelSnapshotsRemover;
import org.elasticsearch.xpack.ml.job.retention.ExpiredResultsRemover;
import org.elasticsearch.xpack.ml.job.retention.MlDataRemover;
import org.elasticsearch.xpack.ml.job.retention.UnusedStateRemover;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
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

    private static final Logger logger = LogManager.getLogger(TransportDeleteExpiredDataAction.class);

    static final Duration DEFAULT_MAX_DURATION = Duration.ofHours(8);

    private final ThreadPool threadPool;
    private final String executor;
    private final OriginSettingClient client;
    private final ClusterService clusterService;
    private final Clock clock;

    @Inject
    public TransportDeleteExpiredDataAction(ThreadPool threadPool, TransportService transportService,
                                            ActionFilters actionFilters, Client client, ClusterService clusterService) {
        this(threadPool, MachineLearning.UTILITY_THREAD_POOL_NAME, transportService, actionFilters, client, clusterService,
            Clock.systemUTC());
    }

    TransportDeleteExpiredDataAction(ThreadPool threadPool, String executor, TransportService transportService,
                                     ActionFilters actionFilters, Client client, ClusterService clusterService, Clock clock) {
        super(DeleteExpiredDataAction.NAME, transportService, actionFilters, DeleteExpiredDataAction.Request::new, executor);
        this.threadPool = threadPool;
        this.executor = executor;
        this.client = new OriginSettingClient(client, ClientHelper.ML_ORIGIN);
        this.clusterService = clusterService;
        this.clock = clock;
    }

    @Override
    protected void doExecute(Task task, DeleteExpiredDataAction.Request request,
                             ActionListener<DeleteExpiredDataAction.Response> listener) {
        logger.info("Deleting expired data");
        Instant timeoutTime = Instant.now(clock).plus(
            request.getTimeout() == null ? DEFAULT_MAX_DURATION : Duration.ofMillis(request.getTimeout().millis())
        );

        Supplier<Boolean> isTimedOutSupplier = () -> Instant.now(clock).isAfter(timeoutTime);
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(
            () -> deleteExpiredData(request, listener, isTimedOutSupplier)
        );
    }

    private void deleteExpiredData(DeleteExpiredDataAction.Request request,
                                   ActionListener<DeleteExpiredDataAction.Response> listener,
                                   Supplier<Boolean> isTimedOutSupplier) {
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client, clusterService.getNodeName());
        List<MlDataRemover> dataRemovers = Arrays.asList(
                new ExpiredResultsRemover(client, auditor, threadPool),
                new ExpiredForecastsRemover(client, threadPool),
                new ExpiredModelSnapshotsRemover(client, threadPool),
                new UnusedStateRemover(client, clusterService),
                new EmptyStateIndexRemover(client)
        );
        Iterator<MlDataRemover> dataRemoversIterator = new VolatileCursorIterator<>(dataRemovers);
        // If there is no throttle provided, default to none
        float requestsPerSec = request.getRequestsPerSecond() == null ? Float.POSITIVE_INFINITY : request.getRequestsPerSecond();
        int numberOfDatanodes = Math.max(clusterService.state().getNodes().getDataNodes().size(), 1);
        if (requestsPerSec == -1.0f) {
            // With DEFAULT_SCROLL_SIZE = 1000 and a single data node this implies we spread deletion of
            //   1 million documents over 5000 seconds ~= 83 minutes.
            // If we have > 5 data nodes, we don't set our throttling.
            requestsPerSec = numberOfDatanodes < 5 ?
                (float)(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE / 5) * numberOfDatanodes :
                Float.POSITIVE_INFINITY;
        }
        deleteExpiredData(dataRemoversIterator, requestsPerSec, listener, isTimedOutSupplier, true);
    }

    void deleteExpiredData(Iterator<MlDataRemover> mlDataRemoversIterator,
                           float requestsPerSecond,
                           ActionListener<DeleteExpiredDataAction.Response> listener,
                           Supplier<Boolean> isTimedOutSupplier,
                           boolean haveAllPreviousDeletionsCompleted) {
        if (haveAllPreviousDeletionsCompleted && mlDataRemoversIterator.hasNext()) {
            MlDataRemover remover = mlDataRemoversIterator.next();
            ActionListener<Boolean> nextListener = ActionListener.wrap(
                    booleanResponse ->
                        deleteExpiredData(
                            mlDataRemoversIterator,
                            requestsPerSecond,
                            listener,
                            isTimedOutSupplier,
                            booleanResponse
                        ),
                    listener::onFailure);
            // Removing expired ML data and artifacts requires multiple operations.
            // These are queued up and executed sequentially in the action listener,
            // the chained calls must all run the ML utility thread pool NOT the thread
            // the previous action returned in which in the case of a transport_client_boss
            // thread is a disaster.
            remover.remove(requestsPerSecond, new ThreadedActionListener<>(logger, threadPool, executor, nextListener, false),
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
