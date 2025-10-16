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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.IMPLEMENTED_TASK_TYPES;

public class ElasticInferenceServiceAuthorizationHandlerV2 implements Closeable {
    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceAuthorizationHandlerV2.class);

    private final ServiceComponents serviceComponents;
    private final ModelRegistry modelRegistry;
    private final ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler;
    private final CountDownLatch firstAuthorizationCompletedLatch = new CountDownLatch(1);
    private final Sender sender;
    private final Runnable callback;
    private final AtomicReference<Scheduler.ScheduledCancellable> lastAuthTask = new AtomicReference<>(null);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ElasticInferenceServiceSettings elasticInferenceServiceSettings;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;

    public ElasticInferenceServiceAuthorizationHandlerV2(
        ServiceComponents serviceComponents,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ElasticInferenceServiceComponents components,
        ModelRegistry modelRegistry
    ) {
        this(
            serviceComponents,
            authorizationRequestHandler,
            sender,
            elasticInferenceServiceSettings,
            components,
            modelRegistry,
            null
        );
    }

    // default for testing
    ElasticInferenceServiceAuthorizationHandlerV2(
        ServiceComponents serviceComponents,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        ElasticInferenceServiceComponents components,
        ModelRegistry modelRegistry,
        // this is a hack to facilitate testing
        Runnable callback
    ) {
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.authorizationHandler = Objects.requireNonNull(authorizationRequestHandler);
        this.sender = Objects.requireNonNull(sender);
        this.elasticInferenceServiceSettings = Objects.requireNonNull(elasticInferenceServiceSettings);
        this.elasticInferenceServiceComponents = Objects.requireNonNull(components);
        this.modelRegistry = Objects.requireNonNull(modelRegistry);
        this.callback = callback;
    }

    public void init() {
        if (initialized.compareAndSet(false, true)) {
            logger.debug("Initializing authorization logic");
            serviceComponents.threadPool().executor(UTILITY_THREAD_POOL_NAME).execute(this::scheduleAndSendAuthorizationRequest);
        }
    }

    /**
     * Waits the specified amount of time for the first authorization call to complete. This is mainly to make testing easier.
     * @param waitTime the max time to wait
     * @throws IllegalStateException if the wait time is exceeded or the call receives an {@link InterruptedException}
     */
    public void waitForAuthorizationToComplete(TimeValue waitTime) {
        try {
            if (firstAuthorizationCompletedLatch.await(waitTime.getSeconds(), TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("The wait time has expired for authorization to complete.");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Waiting for authorization to complete was interrupted");
        }
    }

    @Override
    public void close() throws IOException {
        shutdown.set(true);
        if (lastAuthTask.get() != null) {
            lastAuthTask.get().cancel();
        }
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
        }
    }

    private void scheduleAndSendAuthorizationRequest() {
        if (shutdown.get()) {
            return;
        }

        scheduleAuthorizationRequest();
        sendAuthorizationRequest();
    }

    private void sendAuthorizationRequest() {
        var finalListener = ActionListener.<Void>running(() -> {
            if (callback != null) {
                callback.run();
            }
            firstAuthorizationCompletedLatch.countDown();
        }).delegateResponse((delegate, e) -> {
            logger.atWarn().withThrowable(e).log("Failed processing EIS preconfigured endpoints");
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

        var modelsToAdd = PreconfiguredEndpointModelAdapter.getModels(newInferenceIds, elasticInferenceServiceComponents);

        ActionListener<List<ModelRegistry.ModelStoreResponse>> storeListener = ActionListener.wrap(responses -> {
            for (var response : responses) {
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

        modelRegistry.storeModels(
            modelsToAdd,
            ActionListener.runAfter(storeListener, () -> listener.onResponse(null)),
            TimeValue.THIRTY_SECONDS
        );
    }
}
