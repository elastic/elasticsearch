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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.common.BroadcastMessageAction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller.TASK_NAME;

/**
 * Handles creating a persistent task that will periodically poll the Elastic Inference Service for which models are authorized.
 * A cluster state listener is run on each node to ensure that the persistent task is created. Only one task will exist within the cluster.
 * The task will only be created if CCM cannot be configured or if CCM is configurable (for an on-prem cluster) and is enabled.
 * When a user enables CCM the logic will immediately try to create the persistent task
 * to avoid having to wait for the next cluster update.
 */
public class AuthorizationTaskExecutor extends PersistentTasksExecutor<AuthorizationTaskParams> implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AuthorizationTaskExecutor.class);

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final AuthorizationPoller.Parameters pollerParameters;
    private final AtomicReference<AuthorizationPoller> currentTask = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FeatureService featureService;

    public static AuthorizationTaskExecutor create(
        ClusterService clusterService,
        FeatureService featureService,
        AuthorizationPoller.Parameters parameters
    ) {
        Objects.requireNonNull(clusterService);
        Objects.requireNonNull(parameters);

        return new AuthorizationTaskExecutor(
            clusterService,
            new PersistentTasksService(clusterService, parameters.serviceComponents().threadPool(), parameters.client()),
            featureService,
            parameters
        );
    }

    // default for testing
    AuthorizationTaskExecutor(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        FeatureService featureService,
        AuthorizationPoller.Parameters pollerParameters
    ) {
        super(TASK_NAME, pollerParameters.serviceComponents().threadPool().executor(UTILITY_THREAD_POOL_NAME));
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
        this.persistentTasksService = Objects.requireNonNull(persistentTasksService);
        this.pollerParameters = Objects.requireNonNull(pollerParameters);
    }

    /**
     * Starts the authorization task executor without starting the persistent task. The persistent task will be created
     * when the next cluster state change event occurs. This is needed because
     * we can't start the persistent task until after the plugin has finished initializing. Otherwise, we'll
     * get an error indicating that it isn't aware of whether the task is a cluster scoped task.
     */
    public synchronized void startAndLazyCreateTask() {
        startInternal(false);
    }

    /**
     * Starts the authorization task executor and creates the persistent task if it doesn't already exist. This should only be called from
     * a context where the cluster state is already initialized. Don't call this from the plugin
     * {@link org.elasticsearch.xpack.inference.InferencePlugin#createComponents(Plugin.PluginServices)}. Use
     * {@link #startAndLazyCreateTask()} instead.
     */
    public synchronized void startAndImmediatelyCreateTask() {
        startInternal(true);
    }

    private void startInternal(boolean createPersistentTask) {
        var eisUrl = pollerParameters.elasticInferenceServiceSettings().getElasticInferenceServiceUrl();

        logger.info("Authorization task executor EIS URL: [{}]", eisUrl);

        // If the EIS url is not configured, then we won't be able to interact with the service, so don't start the task.
        if (Strings.isNullOrEmpty(eisUrl) == false && running.compareAndSet(false, true)) {
            logger.info("Starting authorization task executor");

            if (createPersistentTask) {
                sendStartRequest(clusterService.state());
            }

            clusterService.addListener(this);
        }
    }

    private void sendStartRequest(@Nullable ClusterState state) {
        if (shouldSkipCreatingTask(state)) {
            return;
        }

        persistentTasksService.sendClusterStartRequest(
            TASK_NAME,
            TASK_NAME,
            AuthorizationTaskParams.INSTANCE,
            TimeValue.THIRTY_SECONDS,
            ActionListener.wrap(
                persistentTask -> logger.info("Finished creating authorization poller task, id {}", persistentTask.getId()),
                exception -> {
                    var thrownException = exception instanceof RemoteTransportException ? exception.getCause() : exception;
                    if (thrownException instanceof ResourceAlreadyExistsException == false) {
                        logger.error("Failed to create authorization poller task", exception);
                    }
                }
            )
        );
    }

    private boolean shouldSkipCreatingTask(@Nullable ClusterState state) {
        if (state == null) {
            return true;
        }

        return clusterCanSupportFeature(state) == false || running.get() == false || authorizationTaskExists(state);
    }

    private boolean clusterCanSupportFeature(@Nullable ClusterState state) {
        if (state == null) {
            return false;
        }

        return state.clusterRecovered() && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_AUTH_POLLER_PERSISTENT_TASK);
    }

    private static boolean authorizationTaskExists(@Nullable ClusterState state) {
        if (state == null) {
            return false;
        }

        return ClusterPersistentTasksCustomMetadata.getTaskWithId(state, TASK_NAME) != null;
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Shutting down authorization task executor");
            clusterService.removeListener(this);

            sendStopRequest();
        }
    }

    private void sendStopRequest() {
        persistentTasksService.sendClusterRemoveRequest(
            TASK_NAME,
            TimeValue.THIRTY_SECONDS,
            ActionListener.wrap(
                persistentTask -> logger.info("Stopped authorization poller task, id {}", persistentTask.getId()),
                exception -> {
                    var thrownException = exception instanceof RemoteTransportException ? exception.getCause() : exception;
                    if (thrownException instanceof ResourceNotFoundException == false) {
                        logger.error("Failed to stop authorization poller task", exception);
                    }
                }
            )
        );
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
        logger.info("Started authorization poller task with id {}", task.getId());
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
        sendStartRequest(event.state());
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

    /**
     * This action is used to broadcast to all the nodes that the authorization task executor should start or stop.
     * This is specifically useful for CCM, since whether to do the polling depends on the CCM
     * configuration to exist first.
     */
    public static class Action extends BroadcastMessageAction<Message> {
        public static final String NAME = "cluster:internal/xpack/inference/update_authorization_task";
        public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

        private final AuthorizationTaskExecutor authorizationTaskExecutor;

        @Inject
        public Action(
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            AuthorizationTaskExecutor authorizationTaskExecutor
        ) {
            super(NAME, clusterService, transportService, actionFilters, Message::new);
            this.authorizationTaskExecutor = authorizationTaskExecutor;
        }

        @Override
        protected void receiveMessage(Message message) {
            if (message.enable()) {
                authorizationTaskExecutor.startAndImmediatelyCreateTask();
            } else {
                authorizationTaskExecutor.stop();
            }
        }
    }

    public record Message(boolean enable) implements Writeable {
        public static final Message ENABLE_MESSAGE = new Message(true);
        public static final Message DISABLE_MESSAGE = new Message(false);

        public Message(StreamInput in) throws IOException {
            this(in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enable);
        }
    }
}
