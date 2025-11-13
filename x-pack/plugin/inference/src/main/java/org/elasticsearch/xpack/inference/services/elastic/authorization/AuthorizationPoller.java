/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.IMPLEMENTED_TASK_TYPES;

public class AuthorizationPoller extends AllocatedPersistentTask {

    public static final String TASK_NAME = "eis-authorization-poller";

    private static final Logger logger = LogManager.getLogger(AuthorizationPoller.class);

    private final ServiceComponents serviceComponents;
    private final ModelRegistry modelRegistry;
    private final ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler;
    private final Sender sender;
    private final Runnable callback;
    private final AtomicReference<Scheduler.ScheduledCancellable> lastAuthTask = new AtomicReference<>(null);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ElasticInferenceServiceSettings elasticInferenceServiceSettings;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;
    private final Client client;
    private final CountDownLatch receivedFirstAuthResponseLatch = new CountDownLatch(1);

    public record TaskFields(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {}

    public record Parameters(
        ServiceComponents serviceComponents,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ModelRegistry modelRegistry,
        Client client
    ) {}

    public static AuthorizationPoller create(TaskFields taskFields, Parameters parameters) {
        return new AuthorizationPoller(Objects.requireNonNull(taskFields), Objects.requireNonNull(parameters));
    }

    private AuthorizationPoller(TaskFields taskFields, Parameters parameters) {
        this(
            taskFields,
            parameters.serviceComponents,
            parameters.authorizationRequestHandler,
            parameters.sender,
            parameters.elasticInferenceServiceSettings,
            parameters.modelRegistry,
            parameters.client,
            null
        );
    }

    // default for testing
    AuthorizationPoller(
        TaskFields taskFields,
        ServiceComponents serviceComponents,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ModelRegistry modelRegistry,
        Client client,
        // this is a hack to facilitate testing
        Runnable callback
    ) {
        super(taskFields.id, taskFields.type, taskFields.action, taskFields.description, taskFields.parentTask, taskFields.headers);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.authorizationHandler = Objects.requireNonNull(authorizationRequestHandler);
        this.sender = Objects.requireNonNull(sender);
        this.elasticInferenceServiceSettings = Objects.requireNonNull(elasticInferenceServiceSettings);
        this.elasticInferenceServiceComponents = new ElasticInferenceServiceComponents(
            elasticInferenceServiceSettings.getElasticInferenceServiceUrl()
        );
        this.modelRegistry = Objects.requireNonNull(modelRegistry);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
        this.callback = callback;
    }

    public void start() {
        if (initialized.compareAndSet(false, true)) {
            logger.debug("Initializing EIS authorization logic");
            serviceComponents.threadPool().executor(UTILITY_THREAD_POOL_NAME).execute(this::scheduleAndSendAuthorizationRequest);
        }
    }

    /**
     * This should only be used for testing to wait for the first authorization response to be received.
     */
    public void waitForAuthorizationToComplete(TimeValue waitTime) {
        try {
            if (receivedFirstAuthResponseLatch.await(waitTime.getSeconds(), TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("The wait time has expired for first authorization response to be received.");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Waiting for first authorization response to complete was interrupted");
        }
    }

    // Overriding so tests in the same package can access
    @Override
    protected void init(
        PersistentTasksService persistentTasksService,
        TaskManager taskManager,
        String persistentTaskId,
        long allocationId
    ) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId);
    }

    @Override
    protected void onCancelled() {
        shutdown();
        markAsCompleted();
    }

    private void shutdownAndMarkTaskAsFailed(Exception e) {
        shutdown();
        markAsFailed(e);
    }

    // default for testing
    void shutdown() {
        shutdown.set(true);

        var authTask = lastAuthTask.get();
        if (authTask != null) {
            authTask.cancel();
        }
    }

    // default for testing
    boolean isShutdown() {
        return shutdown.get();
    }

    private void scheduleAuthorizationRequest() {
        try {
            if (elasticInferenceServiceSettings.isPeriodicAuthorizationEnabled() == false) {
                return;
            }

            // this call has to be on the individual thread otherwise we get an exception
            var random = Randomness.get();
            var jitter = (long) (elasticInferenceServiceSettings.getMaxAuthorizationRequestJitter().millis() * random.nextDouble());
            var waitTime = TimeValue.timeValueMillis(elasticInferenceServiceSettings.getAuthRequestInterval().millis() + jitter);

            logger.debug(
                () -> Strings.format(
                    "Scheduling the next authorization call with request interval: %s ms, jitter: %d ms",
                    elasticInferenceServiceSettings.getAuthRequestInterval().millis(),
                    jitter
                )
            );
            logger.debug(() -> Strings.format("Next authorization call in %d minutes", waitTime.getMinutes()));

            lastAuthTask.set(
                serviceComponents.threadPool()
                    .schedule(
                        this::scheduleAndSendAuthorizationRequest,
                        waitTime,
                        serviceComponents.threadPool().executor(UTILITY_THREAD_POOL_NAME)
                    )
            );
        } catch (Exception e) {
            logger.warn("Failed scheduling authorization request", e);
            // Shutdown and complete the task so it will be restarted
            shutdownAndMarkTaskAsFailed(e);
        }
    }

    private void scheduleAndSendAuthorizationRequest() {
        if (shutdown.get()) {
            return;
        }

        scheduleAuthorizationRequest();
        sendAuthorizationRequest();
    }

    // default for testing
    void sendAuthorizationRequest() {
        if (modelRegistry.isReady() == false) {
            return;
        }

        var finalListener = ActionListener.<Void>running(() -> {
            if (callback != null) {
                callback.run();
            }
            receivedFirstAuthResponseLatch.countDown();
        }).delegateResponse((delegate, e) -> {
            logger.atWarn().withThrowable(e).log("Failed processing EIS preconfigured endpoints");
            delegate.onResponse(null);
        });

        SubscribableListener.<ElasticInferenceServiceAuthorizationModel>newForked(
            authModelListener -> authorizationHandler.getAuthorization(authModelListener, sender)
        )
            .andThenApply(this::getNewInferenceEndpointsToStore)
            .<Void>andThen((storeListener, newInferenceIds) -> storePreconfiguredModels(newInferenceIds, storeListener))
            .addListener(finalListener);
    }

    private Set<String> getNewInferenceEndpointsToStore(ElasticInferenceServiceAuthorizationModel authModel) {
        var scopedAuthModel = authModel.newLimitedToTaskTypes(EnumSet.copyOf(IMPLEMENTED_TASK_TYPES));

        var authorizedModelIds = scopedAuthModel.getAuthorizedModelIds();
        var existingInferenceIds = modelRegistry.getInferenceIds();

        var newInferenceIds = authorizedModelIds.stream()
            .map(InternalPreconfiguredEndpoints::getWithModelName)
            .filter(Objects::nonNull)
            .map(model -> model.configurations().getInferenceEntityId())
            .collect(Collectors.toSet());

        newInferenceIds.removeAll(existingInferenceIds);
        return newInferenceIds;
    }

    private void storePreconfiguredModels(Set<String> newInferenceIds, ActionListener<Void> listener) {
        if (newInferenceIds.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        logger.info("Storing new EIS preconfigured inference endpoints with inference IDs {}", newInferenceIds);
        var modelsToAdd = PreconfiguredEndpointModelAdapter.getModels(newInferenceIds, elasticInferenceServiceComponents);
        var storeRequest = new StoreInferenceEndpointsAction.Request(modelsToAdd, TimeValue.THIRTY_SECONDS);

        ActionListener<StoreInferenceEndpointsAction.Response> logResultsListener = ActionListener.wrap(responses -> {
            for (var response : responses.getResults()) {
                if (response.failed()) {
                    logger.atWarn()
                        .withThrowable(response.failureCause())
                        .log("Failed to store new EIS preconfigured inference endpoint with inference ID [{}]", response.inferenceId());
                } else {
                    logger.atInfo()
                        .log("Successfully stored EIS preconfigured inference endpoint with inference ID [{}]", response.inferenceId());
                }
            }
        }, e -> logger.atWarn().withThrowable(e).log("Failed to store new EIS preconfigured inference endpoints [{}]", newInferenceIds));

        client.execute(
            StoreInferenceEndpointsAction.INSTANCE,
            storeRequest,
            ActionListener.runAfter(logResultsListener, () -> listener.onResponse(null))
        );
    }
}
