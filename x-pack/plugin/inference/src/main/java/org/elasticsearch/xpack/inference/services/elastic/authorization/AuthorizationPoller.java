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
import org.elasticsearch.inference.Model;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.features.InferenceFeatureService;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
    private final Client client;
    private final CountDownLatch receivedFirstAuthResponseLatch = new CountDownLatch(1);
    private final CCMFeature ccmFeature;
    private final CCMService ccmService;
    private final InferenceFeatureService inferenceFeatureService;

    public record TaskFields(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {}

    public record Parameters(
        ServiceComponents serviceComponents,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ModelRegistry modelRegistry,
        Client client,
        CCMFeature ccmFeature,
        CCMService ccmService,
        InferenceFeatureService inferenceFeatureService
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
            parameters.ccmFeature,
            parameters.ccmService,
            null,
            parameters.inferenceFeatureService
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
        CCMFeature ccmFeature,
        CCMService ccmService,
        // this is a hack to facilitate testing
        Runnable callback,
        InferenceFeatureService inferenceFeatureService
    ) {
        super(taskFields.id, taskFields.type, taskFields.action, taskFields.description, taskFields.parentTask, taskFields.headers);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.authorizationHandler = Objects.requireNonNull(authorizationRequestHandler);
        this.sender = Objects.requireNonNull(sender);
        this.elasticInferenceServiceSettings = Objects.requireNonNull(elasticInferenceServiceSettings);
        this.modelRegistry = Objects.requireNonNull(modelRegistry);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
        this.ccmService = Objects.requireNonNull(ccmService);
        this.callback = callback;
        this.inferenceFeatureService = Objects.requireNonNull(inferenceFeatureService);
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
        shutdownInternal(this::markAsCompleted);
    }

    private void shutdownAndMarkTaskAsFailed(Exception e) {
        shutdownInternal(() -> markAsFailed(e));
    }

    // default for testing
    void shutdown() {
        shutdownInternal(() -> {});
    }

    private void shutdownInternal(Runnable completionRunnable) {
        if (shutdown.compareAndSet(false, true)) {
            // Marking a task as completed and then failed (or vice versa) results in an exception,
            // so we need to ensure only one is called.
            completionRunnable.run();
        }

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
        var finalListener = ActionListener.<Void>running(() -> {
            if (callback != null) {
                callback.run();
            }
            receivedFirstAuthResponseLatch.countDown();
        }).delegateResponse((delegate, e) -> {
            logger.atWarn().withThrowable(e).log("Failed processing EIS preconfigured endpoints");
            delegate.onResponse(null);
        });

        shouldSendAuthRequest(ActionListener.wrap(action -> action.accept(finalListener), e -> {
            logger.atWarn().withThrowable(e).log("Failed determining whether to send authorization request");
            finalListener.onFailure(e);
        }));
    }

    private class ShutdownAction implements Consumer<ActionListener<Void>> {
        @Override
        public void accept(ActionListener<Void> listener) {
            logger.info("Skipping sending authorization request and completing task, because poller is shutting down");
            // We should already be shutdown, so this should just be a noop
            shutdownInternal(AuthorizationPoller.this::markAsCompleted);
            listener.onResponse(null);
        }
    }

    private record SkipAndLogAction(String reason) implements Consumer<ActionListener<Void>> {
        private static final SkipAndLogAction REGISTRY_NOT_READY_ACTION = new SkipAndLogAction("the model registry is not ready");
        private static final SkipAndLogAction MISSING_REQUIRED_FEATURES = new SkipAndLogAction(
            "the cluster is currently upgrading and missing required features"
        );

        @Override
        public void accept(ActionListener<Void> listener) {
            logger.info("Skipping sending authorization request, because {}", reason);
            listener.onResponse(null);
        }
    }

    private class SendAuthRequestAction implements Consumer<ActionListener<Void>> {
        @Override
        public void accept(ActionListener<Void> listener) {
            sendRequest(listener);
        }
    }

    private class CCMDisabledAction implements Consumer<ActionListener<Void>> {
        @Override
        public void accept(ActionListener<Void> listener) {
            logger.info("Skipping sending authorization request and completing task, because CCM is not enabled");
            shutdownInternal(AuthorizationPoller.this::markAsCompleted);
            listener.onResponse(null);
        }
    }

    private void shouldSendAuthRequest(ActionListener<Consumer<ActionListener<Void>>> listener) {
        if (shutdown.get()) {
            listener.onResponse(new ShutdownAction());
            return;
        }
        if (modelRegistry.isReady() == false) {
            listener.onResponse(SkipAndLogAction.REGISTRY_NOT_READY_ACTION);
            return;
        }
        if (inferenceFeatureService.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD) == false) {
            listener.onResponse(SkipAndLogAction.MISSING_REQUIRED_FEATURES);
            return;
        }
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            listener.onResponse(new SendAuthRequestAction());
            return;
        }

        ccmService.isEnabled(listener.delegateFailureAndWrap((delegate, enabled) -> {
            if (enabled == null || enabled == false) {
                delegate.onResponse(new CCMDisabledAction());
                return;
            }
            delegate.onResponse(new SendAuthRequestAction());
        }));
    }

    private void sendRequest(ActionListener<Void> listener) {
        SubscribableListener.<ElasticInferenceServiceAuthorizationModel>newForked(
            authModelListener -> authorizationHandler.getAuthorization(authModelListener, sender)
        )
            .andThenApply(this::getNewInferenceEndpointsToStore)
            .<Void>andThen((storeListener, newInferenceIds) -> storePreconfiguredModels(newInferenceIds, storeListener))
            .addListener(listener);
    }

    private List<Model> getNewInferenceEndpointsToStore(ElasticInferenceServiceAuthorizationModel authModel) {
        logger.debug("Received authorization response, {}", authModel);

        var scopedAuthModel = authModel.newLimitedToTaskTypes(EnumSet.copyOf(IMPLEMENTED_TASK_TYPES));
        logger.debug("Authorization entity limited to service task types, {}", scopedAuthModel);

        var newEndpointIds = new HashSet<>(scopedAuthModel.getEndpointIds());

        var existingInferenceIds = modelRegistry.getInferenceIds();

        newEndpointIds.removeAll(existingInferenceIds);
        return scopedAuthModel.getEndpoints(newEndpointIds);
    }

    private void storePreconfiguredModels(List<Model> newEndpoints, ActionListener<Void> listener) {
        if (newEndpoints.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        logger.info(
            "Storing new EIS preconfigured inference endpoints with inference IDs {}",
            newEndpoints.stream().map(Model::getInferenceEntityId).toList()
        );
        var storeRequest = new StoreInferenceEndpointsAction.Request(newEndpoints, TimeValue.THIRTY_SECONDS);

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
        }, e -> logger.atWarn().withThrowable(e).log("Failed to store new EIS preconfigured inference endpoints [{}]", newEndpoints));

        client.execute(
            StoreInferenceEndpointsAction.INSTANCE,
            storeRequest,
            ActionListener.runAfter(logResultsListener, () -> listener.onResponse(null))
        );
    }
}
