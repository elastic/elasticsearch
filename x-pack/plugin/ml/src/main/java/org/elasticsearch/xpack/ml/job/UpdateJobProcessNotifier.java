/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.utils.VolatileCursorIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.action.UpdateProcessAction.Request;
import static org.elasticsearch.xpack.core.ml.action.UpdateProcessAction.Response;

/**
 * This class serves as a queue for updates to the job process.
 * Queueing is important for 2 reasons: first, it throttles the updates
 * to the process, and second and most important, it preserves the order of the updates
 * for actions that run on the master node. For preserving the order of the updates
 * to the job config, it's necessary to handle the whole update chain on the master
 * node. However, for updates to resources the job uses (e.g. calendars, filters),
 * they can be handled on non-master nodes as long as the update process action
 * is fetching the latest version of those resources from the index instead of
 * using the version that existed while the handling action was at work. This makes
 * sure that even if the order of updates gets reversed, the final process update
 * will fetch the valid state of those external resources ensuring the process is
 * in sync.
 */
public class UpdateJobProcessNotifier {

    private static final Logger logger = LogManager.getLogger(UpdateJobProcessNotifier.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final LinkedBlockingQueue<UpdateHolder> orderedJobUpdates = new LinkedBlockingQueue<>(1000);

    private volatile ThreadPool.Cancellable cancellable;

    public UpdateJobProcessNotifier(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        clusterService.addLifecycleListener(new LifecycleListener() {

            @Override
            public void beforeStart() {
                start();
            }

            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    boolean submitJobUpdate(UpdateParams update, ActionListener<Boolean> listener) {
        return orderedJobUpdates.offer(new UpdateHolder(update, listener));
    }

    private void start() {
        cancellable = threadPool.scheduleWithFixedDelay(this::processNextUpdate, TimeValue.timeValueSeconds(1), ThreadPool.Names.GENERIC);
    }

    private void stop() {
        orderedJobUpdates.clear();

        ThreadPool.Cancellable cancellable = this.cancellable;
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    private void processNextUpdate() {
        List<UpdateHolder> updates = new ArrayList<>(orderedJobUpdates.size());
        try {
            orderedJobUpdates.drainTo(updates);
            executeProcessUpdates(new VolatileCursorIterator<>(updates));
        } catch (Exception e) {
            logger.error("Error while processing next job update", e);
        }
    }

    void executeProcessUpdates(Iterator<UpdateHolder> updatesIterator) {
        if (updatesIterator.hasNext() == false) {
            return;
        }
        UpdateHolder updateHolder = updatesIterator.next();
        UpdateParams update = updateHolder.update;

        if (update.isJobUpdate() && clusterService.localNode().isMasterNode() == false) {
            assert clusterService.localNode().isMasterNode();
            logger.error("Job update was submitted to non-master node [" + clusterService.getNodeName() + "]; update for job ["
                    + update.getJobId() + "] will be ignored");
            executeProcessUpdates(updatesIterator);
            return;
        }

        Request request = new Request(update.getJobId(), update.getModelPlotConfig(), update.getDetectorUpdates(), update.getFilter(),
                update.isUpdateScheduledEvents());

        executeAsyncWithOrigin(client, ML_ORIGIN, UpdateProcessAction.INSTANCE, request,
                new ActionListener<Response>() {
                    @Override
                    public void onResponse(Response response) {
                        if (response.isUpdated()) {
                            logger.info("Successfully updated remote job [{}]", update.getJobId());
                            updateHolder.listener.onResponse(true);
                        } else {
                            String msg = "Failed to update remote job [" + update.getJobId() + "]";
                            logger.error(msg);
                            updateHolder.listener.onFailure(ExceptionsHelper.serverError(msg));
                        }
                        executeProcessUpdates(updatesIterator);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof ResourceNotFoundException) {
                            logger.debug("Remote job [{}] not updated as it has been deleted", update.getJobId());
                        } else if (cause.getMessage().contains("because job [" + update.getJobId() + "] is not open")
                                && cause instanceof ElasticsearchStatusException) {
                            logger.debug("Remote job [{}] not updated as it is no longer open", update.getJobId());
                        } else {
                            logger.error("Failed to update remote job [" + update.getJobId() + "]", cause);
                        }
                        updateHolder.listener.onFailure(e);
                        executeProcessUpdates(updatesIterator);
                    }
                });
    }

    private static class UpdateHolder {
        private final UpdateParams update;
        private final ActionListener<Boolean> listener;

        private UpdateHolder(UpdateParams update, ActionListener<Boolean> listener) {
            this.update = update;
            this.listener = listener;
        }
    }
}
