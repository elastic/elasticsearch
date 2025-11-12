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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller.TASK_NAME;

public class AuthorizationTaskExecutor extends PersistentTasksExecutor<AuthorizationTaskParams> implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AuthorizationTaskExecutor.class);

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final AuthorizationPoller.Parameters pollerParameters;
    private final AtomicReference<AuthorizationPoller> currentTask = new AtomicReference<>();

    public static AuthorizationTaskExecutor create(ClusterService clusterService, AuthorizationPoller.Parameters parameters) {
        Objects.requireNonNull(clusterService);
        Objects.requireNonNull(parameters);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            new PersistentTasksService(clusterService, parameters.serviceComponents().threadPool(), parameters.client()),
            parameters
        );
        executor.init();
        return executor;
    }

    // default for testing
    AuthorizationTaskExecutor(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        AuthorizationPoller.Parameters pollerParameters
    ) {
        super(TASK_NAME, pollerParameters.serviceComponents().threadPool().executor(UTILITY_THREAD_POOL_NAME));
        this.clusterService = Objects.requireNonNull(clusterService);
        this.persistentTasksService = Objects.requireNonNull(persistentTasksService);
        this.pollerParameters = Objects.requireNonNull(pollerParameters);
    }

    // default for testing
    void init() {
        // If the EIS url is not configured, then we won't be able to interact with the service, so don't start the task.
        if (Strings.isNullOrEmpty(pollerParameters.elasticInferenceServiceSettings().getElasticInferenceServiceUrl()) == false) {
            clusterService.addListener(this);
        }
    }

    /**
     * This method should only be used for testing purposes to get the current running task.
     */
    public AuthorizationPoller getCurrentPollerTask() {
        return currentTask.get();
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, AuthorizationTaskParams params, PersistentTaskState state) {
        var authPoller = (AuthorizationPoller) task;
        currentTask.set(authPoller);
        authPoller.start();
    }

    @FixForMultiProject(
        description = "A single cluster can have multiple projects, "
            + "we'll need to either make a call per project/org or use a bulk authorization api that EIS provides"
    )
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
        return AuthorizationPoller.create(
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
            ActionListener.wrap(persistentTask -> logger.debug("Created authorization poller task"), exception -> {
                var thrownException = exception instanceof RemoteTransportException ? exception.getCause() : exception;
                if (thrownException instanceof ResourceAlreadyExistsException == false) {
                    logger.error("Failed to create authorization poller task", exception);
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
