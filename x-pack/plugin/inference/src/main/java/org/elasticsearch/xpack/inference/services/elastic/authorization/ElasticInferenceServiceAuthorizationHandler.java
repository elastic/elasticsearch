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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.DefaultModelConfig;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class ElasticInferenceServiceAuthorizationHandler implements Closeable {
    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceAuthorizationHandler.class);

    private record AuthorizedContent(
        ElasticInferenceServiceAuthorizationModel taskTypesAndModels,
        List<InferenceService.DefaultConfigId> configIds,
        List<DefaultModelConfig> defaultModelConfigs
    ) {
        static AuthorizedContent empty() {
            return new AuthorizedContent(ElasticInferenceServiceAuthorizationModel.newDisabledService(), List.of(), List.of());
        }
    }

    private final ServiceComponents serviceComponents;
    private final AtomicReference<AuthorizedContent> authorizedContent = new AtomicReference<>(AuthorizedContent.empty());
    private final ModelRegistry modelRegistry;
    private final ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler;
    private final AtomicReference<ElasticInferenceService.Configuration> configuration;
    private final Map<String, DefaultModelConfig> defaultModelsConfigs;
    private final CountDownLatch firstAuthorizationCompletedLatch = new CountDownLatch(1);
    private final EnumSet<TaskType> implementedTaskTypes;
    private final InferenceService inferenceService;
    private final Sender sender;
    private final Runnable callback;
    private final AtomicReference<Scheduler.ScheduledCancellable> lastAuthTask = new AtomicReference<>(null);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ElasticInferenceServiceSettings elasticInferenceServiceSettings;

    public ElasticInferenceServiceAuthorizationHandler(
        ServiceComponents serviceComponents,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Map<String, DefaultModelConfig> defaultModelsConfigs,
        EnumSet<TaskType> implementedTaskTypes,
        InferenceService inferenceService,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings
    ) {
        this(
            serviceComponents,
            modelRegistry,
            authorizationRequestHandler,
            defaultModelsConfigs,
            implementedTaskTypes,
            Objects.requireNonNull(inferenceService),
            sender,
            elasticInferenceServiceSettings,
            null
        );
    }

    // default for testing
    ElasticInferenceServiceAuthorizationHandler(
        ServiceComponents serviceComponents,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationRequestHandler,
        Map<String, DefaultModelConfig> defaultModelsConfigs,
        EnumSet<TaskType> implementedTaskTypes,
        InferenceService inferenceService,
        Sender sender,
        ElasticInferenceServiceSettings elasticInferenceServiceSettings,
        // this is a hack to facilitate testing
        Runnable callback
    ) {
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.modelRegistry = Objects.requireNonNull(modelRegistry);
        this.authorizationHandler = Objects.requireNonNull(authorizationRequestHandler);
        this.defaultModelsConfigs = Objects.requireNonNull(defaultModelsConfigs);
        this.implementedTaskTypes = Objects.requireNonNull(implementedTaskTypes);
        // allow the service to be null for testing
        this.inferenceService = inferenceService;
        this.sender = Objects.requireNonNull(sender);
        this.elasticInferenceServiceSettings = Objects.requireNonNull(elasticInferenceServiceSettings);

        configuration = new AtomicReference<>(
            new ElasticInferenceService.Configuration(authorizedContent.get().taskTypesAndModels.getAuthorizedTaskTypes())
        );
        this.callback = callback;
    }

    public void init() {
        logger.debug("Initializing authorization logic");
        serviceComponents.threadPool().executor(UTILITY_THREAD_POOL_NAME).execute(this::scheduleAndSendAuthorizationRequest);
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

    public synchronized Set<TaskType> supportedStreamingTasks() {
        var authorizedStreamingTaskTypes = EnumSet.of(TaskType.CHAT_COMPLETION);
        authorizedStreamingTaskTypes.retainAll(authorizedContent.get().taskTypesAndModels.getAuthorizedTaskTypes());

        return authorizedStreamingTaskTypes;
    }

    public synchronized List<InferenceService.DefaultConfigId> defaultConfigIds() {
        return authorizedContent.get().configIds;
    }

    public synchronized void defaultConfigs(ActionListener<List<Model>> defaultsListener) {
        var models = authorizedContent.get().defaultModelConfigs.stream().map(DefaultModelConfig::model).toList();
        defaultsListener.onResponse(models);
    }

    public synchronized EnumSet<TaskType> supportedTaskTypes() {
        return authorizedContent.get().taskTypesAndModels.getAuthorizedTaskTypes();
    }

    public synchronized boolean hideFromConfigurationApi() {
        return authorizedContent.get().taskTypesAndModels.isAuthorized() == false;
    }

    public synchronized InferenceServiceConfiguration getConfiguration() {
        return configuration.get().get();
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
        try {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = ActionListener.wrap((model) -> {
                setAuthorizedContent(model);
                if (callback != null) {
                    callback.run();
                }
            }, e -> {
                // we don't need to do anything if there was a failure, everything is disabled by default
                firstAuthorizationCompletedLatch.countDown();
            });

            authorizationHandler.getAuthorization(listener, sender);
        } catch (Exception e) {
            logger.warn("Failure while sending the request to retrieve authorization", e);
            // we don't need to do anything if there was a failure, everything is disabled by default
            firstAuthorizationCompletedLatch.countDown();
        }
    }

    private synchronized void setAuthorizedContent(ElasticInferenceServiceAuthorizationModel auth) {
        logger.debug("Received authorization response");
        var authorizedTaskTypesAndModels = authorizedContent.get().taskTypesAndModels.merge(auth)
            .newLimitedToTaskTypes(EnumSet.copyOf(implementedTaskTypes));

        // recalculate which default config ids and models are authorized now
        var authorizedDefaultModelIds = getAuthorizedDefaultModelIds(auth);

        var authorizedDefaultConfigIds = getAuthorizedDefaultConfigIds(authorizedDefaultModelIds, auth);
        var authorizedDefaultModelObjects = getAuthorizedDefaultModelsObjects(authorizedDefaultModelIds);
        authorizedContent.set(
            new AuthorizedContent(authorizedTaskTypesAndModels, authorizedDefaultConfigIds, authorizedDefaultModelObjects)
        );

        configuration.set(new ElasticInferenceService.Configuration(authorizedContent.get().taskTypesAndModels.getAuthorizedTaskTypes()));

        authorizedContent.get().configIds().forEach(modelRegistry::putDefaultIdIfAbsent);
        handleRevokedDefaultConfigs(authorizedDefaultModelIds);
    }

    private Set<String> getAuthorizedDefaultModelIds(ElasticInferenceServiceAuthorizationModel auth) {
        var authorizedModels = auth.getAuthorizedModelIds();
        var authorizedDefaultModelIds = new TreeSet<>(defaultModelsConfigs.keySet());
        authorizedDefaultModelIds.retainAll(authorizedModels);

        return authorizedDefaultModelIds;
    }

    private List<InferenceService.DefaultConfigId> getAuthorizedDefaultConfigIds(
        Set<String> authorizedDefaultModelIds,
        ElasticInferenceServiceAuthorizationModel auth
    ) {
        var authorizedConfigIds = new ArrayList<InferenceService.DefaultConfigId>();
        for (var id : authorizedDefaultModelIds) {
            var modelConfig = defaultModelsConfigs.get(id);
            if (modelConfig != null) {
                if (auth.getAuthorizedTaskTypes().contains(modelConfig.model().getTaskType()) == false) {
                    logger.warn(
                        org.elasticsearch.common.Strings.format(
                            "The authorization response included the default model: %s, "
                                + "but did not authorize the assumed task type of the model: %s. Enabling model.",
                            id,
                            modelConfig.model().getTaskType()
                        )
                    );
                }
                authorizedConfigIds.add(
                    new InferenceService.DefaultConfigId(
                        modelConfig.model().getInferenceEntityId(),
                        modelConfig.settings(),
                        inferenceService
                    )
                );
            }
        }

        authorizedConfigIds.sort(Comparator.comparing(InferenceService.DefaultConfigId::inferenceId));
        return authorizedConfigIds;
    }

    private List<DefaultModelConfig> getAuthorizedDefaultModelsObjects(Set<String> authorizedDefaultModelIds) {
        var authorizedModels = new ArrayList<DefaultModelConfig>();
        for (var id : authorizedDefaultModelIds) {
            var modelConfig = defaultModelsConfigs.get(id);
            if (modelConfig != null) {
                authorizedModels.add(modelConfig);
            }
        }

        authorizedModels.sort(Comparator.comparing(modelConfig -> modelConfig.model().getInferenceEntityId()));
        return authorizedModels;
    }

    private void handleRevokedDefaultConfigs(Set<String> authorizedDefaultModelIds) {
        // if a model was initially returned in the authorization response but is absent, then we'll assume authorization was revoked
        var unauthorizedDefaultModelIds = new HashSet<>(defaultModelsConfigs.keySet());
        unauthorizedDefaultModelIds.removeAll(authorizedDefaultModelIds);

        // get all the default inference endpoint ids for the unauthorized model ids
        var unauthorizedDefaultInferenceEndpointIds = unauthorizedDefaultModelIds.stream()
            .map(defaultModelsConfigs::get) // get all the model configs
            .filter(Objects::nonNull) // limit to only non-null
            .map(modelConfig -> modelConfig.model().getInferenceEntityId()) // get the inference ids
            .collect(Collectors.toSet());

        var deleteInferenceEndpointsListener = ActionListener.<Boolean>wrap(result -> {
            logger.debug(Strings.format("Successfully revoked access to default inference endpoint IDs: %s", unauthorizedDefaultModelIds));
            firstAuthorizationCompletedLatch.countDown();
        }, e -> {
            logger.warn(
                Strings.format("Failed to revoke access to default inference endpoint IDs: %s, error: %s", unauthorizedDefaultModelIds, e)
            );
            firstAuthorizationCompletedLatch.countDown();
        });

        logger.debug("Synchronizing default inference endpoints");
        modelRegistry.removeDefaultConfigs(unauthorizedDefaultInferenceEndpointIds, deleteInferenceEndpointsListener);
    }
}
