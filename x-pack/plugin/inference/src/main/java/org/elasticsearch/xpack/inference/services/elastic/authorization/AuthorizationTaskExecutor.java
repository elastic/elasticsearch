/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller.TASK_NAME;

public class AuthorizationTaskExecutor extends PersistentTasksExecutor<AuthorizationTaskParams> implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AuthorizationTaskExecutor.class);

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final AuthorizationPoller.Parameters pollerParameters;

    public AuthorizationTaskExecutor(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        AuthorizationPoller.Parameters pollerParameters
    ) {
        super(TASK_NAME, threadPool.executor(UTILITY_THREAD_POOL_NAME));
        this.clusterService = Objects.requireNonNull(clusterService);
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.pollerParameters = Objects.requireNonNull(pollerParameters);
    }

    public void init() {
        clusterService.addListener(this);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, AuthorizationTaskParams params, PersistentTaskState state) {
        // TODO remove
        logger.warn("Starting authorization poller task");
        var authPoller = (AuthorizationPoller) task;
        authPoller.start();
    }

    @Override
    public Scope scope() {
        return Scope.CLUSTER;
    }

    @Override
    protected AuthorizationPoller createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<AuthorizationTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(id, type, action, getDescription(taskInProgress), parentTaskId, headers),
            pollerParameters
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (authorizationTaskExists(event)) {
            return;
        }

        persistentTasksService.sendClusterStartRequest(
            TASK_NAME,
            TASK_NAME,
            new AuthorizationTaskParams(),
            TimeValue.THIRTY_SECONDS,
            ActionListener.wrap(persistentTask -> {
                logger.warn("Created authorization poller task");
            }, e -> {
                var t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("Failed to create authorization poller task", e);
                }
            })
        );
    }

    private static boolean authorizationTaskExists(ClusterChangedEvent event) {
        return ClusterPersistentTasksCustomMetadata.getTaskWithId(event.state(), TASK_NAME) != null;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(AuthorizationPoller.TASK_NAME),
                AuthorizationTaskParams::fromXContent
            )
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, AuthorizationPoller.TASK_NAME, AuthorizationTaskParams::new)
        );
    }
}
