/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.common.DiagnosticsCache;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

import java.util.Collection;
import java.util.List;

/**
 * A registry that assembles and caches Inference Endpoints, {@link Model}, for reuse.
 * Models are high read and minimally written, where changes only occur during updates and deletes.
 * The cache is invalidated via the {@link ClearInferenceEndpointCacheAction} so that every node gets the invalidation
 * message.
 */
public class InferenceEndpointRegistry extends DiagnosticsCache<Model> {

    /**
     * The maximum number of keys to store within the cache.
     */
    public static final int DEFAULT_CACHE_WEIGHT = 25;

    private static final Setting<Boolean> INFERENCE_ENDPOINT_CACHE_ENABLED = Setting.boolSetting(
        "xpack.inference.endpoint.cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Setting<Integer> INFERENCE_ENDPOINT_CACHE_WEIGHT = Setting.intSetting(
        "xpack.inference.endpoint.cache.weight",
        DEFAULT_CACHE_WEIGHT,
        Setting.Property.NodeScope
    );

    private static final Setting<TimeValue> INFERENCE_ENDPOINT_CACHE_EXPIRY = Setting.timeSetting(
        "xpack.inference.endpoint.cache.expiry_time",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope
    );

    public static Collection<? extends Setting<?>> getSettingsDefinitions() {
        return List.of(INFERENCE_ENDPOINT_CACHE_ENABLED, INFERENCE_ENDPOINT_CACHE_WEIGHT, INFERENCE_ENDPOINT_CACHE_EXPIRY);
    }

    private static final Logger log = LogManager.getLogger(InferenceEndpointRegistry.class);
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private volatile boolean cacheEnabledViaSetting;

    public InferenceEndpointRegistry(
        ClusterService clusterService,
        Settings settings,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        super(buildCache(settings));
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.projectResolver = projectResolver;
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.cacheEnabledViaSetting = INFERENCE_ENDPOINT_CACHE_ENABLED.get(settings);

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(INFERENCE_ENDPOINT_CACHE_ENABLED, enabled -> this.cacheEnabledViaSetting = enabled);
    }

    private static Cache<InferenceIdAndProject, Model> buildCache(Settings settings) {
        return CacheBuilder.<InferenceIdAndProject, Model>builder()
            .setMaximumWeight(INFERENCE_ENDPOINT_CACHE_WEIGHT.get(settings))
            .setExpireAfterWrite(INFERENCE_ENDPOINT_CACHE_EXPIRY.get(settings))
            .removalListener(
                notification -> log.debug(
                    "Inference Endpoint [{}] of project [{}] was removed from endpoint cache due to [{}]",
                    notification.getKey().inferenceEntityId(),
                    notification.getKey().projectId(),
                    notification.getRemovalReason()
                )
            )
            .build();
    }

    public void getEndpoint(String inferenceEntityId, ActionListener<Model> listener) {
        var key = new InferenceIdAndProject(inferenceEntityId, projectResolver.getProjectId());
        var cachedModel = cacheEnabled() ? cache.get(key) : null;
        if (cachedModel != null) {
            log.trace("Retrieved [{}] from cache.", inferenceEntityId);
            listener.onResponse(cachedModel);
        } else {
            loadFromIndex(key, listener);
        }
    }

    void invalidateAll(ProjectId projectId) {
        if (cacheEnabled()) {
            var cacheKeys = cache.keys().iterator();
            while (cacheKeys.hasNext()) {
                if (cacheKeys.next().projectId().equals(projectId)) {
                    cacheKeys.remove();
                }
            }
        }
    }

    private void loadFromIndex(InferenceIdAndProject idAndProject, ActionListener<Model> listener) {
        modelRegistry.getModelWithSecrets(idAndProject.inferenceEntityId(), listener.delegateFailureAndWrap((l, unparsedModel) -> {
            var service = serviceRegistry.getService(unparsedModel.service())
                .orElseThrow(
                    () -> new ResourceNotFoundException(
                        "Unknown service [{}] for model [{}]",
                        unparsedModel.service(),
                        idAndProject.inferenceEntityId()
                    )
                );

            var model = service.parsePersistedConfig(unparsedModel);

            if (cacheEnabled()) {
                cache.put(idAndProject, model);
            }
            l.onResponse(model);
        }));
    }

    @Override
    public boolean cacheEnabled() {
        return cacheEnabledViaSetting && cacheEnabledViaFeature();
    }

    private boolean cacheEnabledViaFeature() {
        var state = clusterService.state();
        return state.clusterRecovered() && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_ENDPOINT_CACHE);
    }

    public static void refreshCacheOnAllNodes(Client client) {
        client.execute(
            ClearInferenceEndpointCacheAction.INSTANCE,
            new ClearInferenceEndpointCacheAction.Request(),
            ActionListener.wrap(
                ignored -> log.debug("Successfully refreshed inference endpoint cache on all nodes."),
                e -> log.atDebug().withThrowable(e).log("Failed to refresh inference endpoint cache on all nodes.")
            )
        );
    }
}
