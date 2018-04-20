/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
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

public class UpdateJobProcessNotifier extends AbstractComponent implements LocalNodeMasterListener {

    private final Client client;
    private final ThreadPool threadPool;
    private final LinkedBlockingQueue<UpdateHolder> orderedJobUpdates = new LinkedBlockingQueue<>(1000);

    private volatile ThreadPool.Cancellable cancellable;

    public UpdateJobProcessNotifier(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        clusterService.addLocalNodeMasterListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    boolean submitJobUpdate(UpdateParams update, ActionListener<Boolean> listener) {
        return orderedJobUpdates.offer(new UpdateHolder(update, listener));
    }

    @Override
    public void onMaster() {
        start();
    }

    @Override
    public void offMaster() {
        stop();
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

    @Override
    public String executorName() {
        // SAME is ok here, because both start() and stop() are inexpensive:
        return ThreadPool.Names.SAME;
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
                        if (e instanceof ResourceNotFoundException) {
                            logger.debug("Remote job [{}] not updated as it has been deleted", update.getJobId());
                        } else if (e.getMessage().contains("because job [" + update.getJobId() + "] is not open")
                                && e instanceof ElasticsearchStatusException) {
                            logger.debug("Remote job [{}] not updated as it is no longer open", update.getJobId());
                        } else {
                            logger.error("Failed to update remote job [" + update.getJobId() + "]", e);
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
