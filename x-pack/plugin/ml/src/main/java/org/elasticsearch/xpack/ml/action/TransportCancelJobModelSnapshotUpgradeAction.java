/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher.SimpleIdsMatcher;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CancelJobModelSnapshotUpgradeAction;
import org.elasticsearch.xpack.core.ml.action.CancelJobModelSnapshotUpgradeAction.Request;
import org.elasticsearch.xpack.core.ml.action.CancelJobModelSnapshotUpgradeAction.Response;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TransportCancelJobModelSnapshotUpgradeAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportCancelJobModelSnapshotUpgradeAction.class);

    private final JobConfigProvider jobConfigProvider;
    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportCancelJobModelSnapshotUpgradeAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobConfigProvider jobConfigProvider,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService
    ) {
        super(CancelJobModelSnapshotUpgradeAction.NAME, transportService, actionFilters, Request::new);
        this.jobConfigProvider = jobConfigProvider;
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    public void doExecute(Task task, Request request, ActionListener<Response> listener) {

        logger.debug("[{}] cancel model snapshot [{}] upgrades", request.getJobId(), request.getSnapshotId());

        // 2. Now that we have the job IDs, find the relevant model snapshot upgrade tasks
        ActionListener<List<Job.Builder>> expandIdsListener = ActionListener.wrap(jobs -> {
            SimpleIdsMatcher matcher = new SimpleIdsMatcher(request.getSnapshotId());
            Set<String> jobIds = jobs.stream().map(Job.Builder::getId).collect(Collectors.toSet());
            PersistentTasksCustomMetadata tasksInProgress = clusterService.state().metadata().custom(PersistentTasksCustomMetadata.TYPE);
            // allow_no_match plays no part here. The reason is that we have a principle that stopping
            // a stopped entity is a no-op, and upgrades that have already completed won't have a task.
            // This is a bit different to jobs and datafeeds, where the entity continues to exist even
            // after it's stopped. Upgrades cease to exist after they're stopped so the match validation
            // cannot be as thorough.
            List<PersistentTasksCustomMetadata.PersistentTask<?>> upgradeTasksToCancel = MlTasks.snapshotUpgradeTasks(tasksInProgress)
                .stream()
                .filter(t -> jobIds.contains(((SnapshotUpgradeTaskParams) t.getParams()).getJobId()))
                .filter(t -> matcher.idMatches(((SnapshotUpgradeTaskParams) t.getParams()).getSnapshotId()))
                .collect(Collectors.toList());
            removePersistentTasks(request, upgradeTasksToCancel, listener);
        }, listener::onFailure);

        // 1. Expand jobs - this will throw if a required job ID match isn't made. Jobs being deleted are included here.
        jobConfigProvider.expandJobs(request.getJobId(), request.allowNoMatch(), false, null, expandIdsListener);
    }

    private void removePersistentTasks(
        Request request,
        List<PersistentTasksCustomMetadata.PersistentTask<?>> upgradeTasksToCancel,
        ActionListener<Response> listener
    ) {
        final int numberOfTasks = upgradeTasksToCancel.size();
        if (numberOfTasks == 0) {
            listener.onResponse(new Response(true));
            return;
        }

        final AtomicInteger counter = new AtomicInteger();
        final AtomicArray<Exception> failures = new AtomicArray<>(numberOfTasks);

        for (PersistentTasksCustomMetadata.PersistentTask<?> task : upgradeTasksToCancel) {
            persistentTasksService.sendRemoveRequest(task.getId(), new ActionListener<>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> task) {
                    if (counter.incrementAndGet() == numberOfTasks) {
                        sendResponseOrFailure(listener, failures);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    final int slot = counter.incrementAndGet();
                    // Not found is not an error - it just means the upgrade completed before we could cancel it.
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException == false) {
                        failures.set(slot - 1, e);
                    }
                    if (slot == numberOfTasks) {
                        sendResponseOrFailure(listener, failures);
                    }
                }

                private void sendResponseOrFailure(ActionListener<Response> listener, AtomicArray<Exception> failures) {
                    List<Exception> caughtExceptions = failures.asList();
                    if (caughtExceptions.isEmpty()) {
                        listener.onResponse(new Response(true));
                        return;
                    }

                    String msg = "Failed to cancel model snapshot upgrade for ["
                        + request.getSnapshotId()
                        + "] on job ["
                        + request.getJobId()
                        + "]. Total failures ["
                        + caughtExceptions.size()
                        + "], rethrowing first, all Exceptions: ["
                        + caughtExceptions.stream().map(Exception::getMessage).collect(Collectors.joining(", "))
                        + "]";

                    ElasticsearchException e = new ElasticsearchException(msg, caughtExceptions.get(0));
                    listener.onFailure(e);
                }
            });
        }
    }
}
