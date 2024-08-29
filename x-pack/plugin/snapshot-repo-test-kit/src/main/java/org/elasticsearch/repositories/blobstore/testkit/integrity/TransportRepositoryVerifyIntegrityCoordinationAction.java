/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.Executor;

public class TransportRepositoryVerifyIntegrityCoordinationAction extends TransportAction<
    TransportRepositoryVerifyIntegrityCoordinationAction.Request,
    RepositoryVerifyIntegrityResponse> {

    /*
     * Message flow: the coordinating node (the one running this action) forwards the request on to a master node which actually runs the
     * verification. The master node in turn sends requests back to this node containing chunks of response, either information about the
     * snapshots processed, or about the restorability of the indices in the repository, or details of any verification anomalies found.
     * When the process is complete the master responds to the original transport request with the final results:
     *
     * +---------+                         +-------------+                           +--------+
     * | Client  |                         | Coordinator |                           | Master |
     * +---------+                         +-------------+                           +--------+
     *      |                                     |                                       |
     *      |-[REST request]--------------------->|                                       |
     *      |                                     |---[master node request]-------------->| ----------------------\
     *      |                                     |                                       |-| Initialize verifier |
     *      |                                     |                                       | |---------------------|
     *      |                                     |<--[response chunk request]------------|
     *      |<---[headers & initial JSON body]----|                                       |
     *      |                                     |---[SNAPSHOT_INFO chunk response]----->| ------------------\
     *      |                                     |                                       |-| Verify snapshot |
     *      |                                     |                                       | |-----------------|
     *      |                                     |<--[SNAPSHOT_INFO chunk request]-------|
     *      |<---[more JSON body]-----------------|                                       |
     *      |                                     |---[SNAPSHOT_INFO chunk response]----->| ------------------\
     *      |                                     |                                       |-| Verify snapshot |
     *      |                                     |                                       | |-----------------|
     *      |                                     |<--[SNAPSHOT_INFO chunk request]-------|
     *      |<---[more JSON body]-----------------|                                       |
     *      |                                     |---[SNAPSHOT_INFO chunk response]----->| ...
     *      .                                     .                                       .
     *      .                                     .                                       .
     *      |                                     |                                       | -----------------------------\
     *      |                                     |                                       |-| Verify index restorability |
     *      |                                     |                                       | |----------------------------|
     *      |                                     |<--[INDEX_RESTORABILITY chunk req.]----|
     *      |<---[more JSON body]-----------------|                                       |
     *      |                                     |---[INDEX_RESTORABILITY chunk resp.]-->| -----------------------------\
     *      |                                     |                                       |-| Verify index restorability |
     *      |                                     |                                       | |----------------------------|
     *      |                                     |<--[INDEX_RESTORABILITY chunk req.]----|
     *      |<---[more JSON body]-----------------|                                       |
     *      |                                     |---[INDEX_RESTORABILITY chunk resp.]-->| ...
     *      .                                     .                                       .
     *      .                                     .                                       .
     *      |                                     |<--[response to master node req.]------|
     *      |<--[final JSON to complete body]-----|                                       |
     *
     * This message flow ties the lifecycle of the verification process to that of the transport request sent from coordinator to master,
     * which means it integrates well with the tasks framework and handles network issues properly. An alternative would be for the
     * coordinator to repeatedly request chunks from the master, but that would mean that there's no one task representing the whole
     * process, and it'd be a little tricky for the master node to know if the coordinator has failed and the verification should be
     * cancelled.
     */

    public static final ActionType<RepositoryVerifyIntegrityResponse> INSTANCE = new ActionType<>(
        "cluster:admin/repository/verify_integrity"
    );

    private final ActiveRepositoryVerifyIntegrityTasks activeRepositoryVerifyIntegrityTasks = new ActiveRepositoryVerifyIntegrityTasks();

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Executor managementExecutor;

    public static class Request extends ActionRequest {
        private final RepositoryVerifyIntegrityParams requestParams;
        private final RepositoryVerifyIntegrityResponseStream responseBuilder;

        public Request(RepositoryVerifyIntegrityParams requestParams, RepositoryVerifyIntegrityResponseStream responseBuilder) {
            this.requestParams = requestParams;
            this.responseBuilder = responseBuilder;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public RepositoryVerifyIntegrityParams requestParams() {
            return requestParams;
        }

        public RepositoryVerifyIntegrityResponseStream responseBuilder() {
            return responseBuilder;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    @Inject
    public TransportRepositoryVerifyIntegrityCoordinationAction(
        TransportService transportService,
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters
    ) {
        super(
            INSTANCE.name(),
            actionFilters,
            transportService.getTaskManager(),
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );

        this.transportService = transportService;
        this.clusterService = clusterService;
        this.managementExecutor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);

        // register subsidiary actions
        new TransportRepositoryVerifyIntegrityMasterNodeAction(transportService, repositoriesService, actionFilters, managementExecutor);

        new TransportRepositoryVerifyIntegrityResponseChunkAction(
            transportService,
            actionFilters,
            managementExecutor,
            activeRepositoryVerifyIntegrityTasks
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<RepositoryVerifyIntegrityResponse> listener) {
        ActionListener.run(
            ActionListener.releaseAfter(
                listener,
                activeRepositoryVerifyIntegrityTasks.registerResponseBuilder(task.getId(), request.responseBuilder())
            ),
            l -> {
                final var master = clusterService.state().nodes().getMasterNode();
                if (master == null) {
                    // no waiting around or retries here, we just fail immediately
                    throw new MasterNotDiscoveredException();
                }
                transportService.sendChildRequest(
                    master,
                    TransportRepositoryVerifyIntegrityMasterNodeAction.ACTION_NAME,
                    new TransportRepositoryVerifyIntegrityMasterNodeAction.Request(
                        transportService.getLocalNode(),
                        task.getId(),
                        request.requestParams()
                    ),
                    task,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(l, RepositoryVerifyIntegrityResponse::new, managementExecutor)
                );
            }
        );
    }
}
