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

import java.util.Collection;
import java.util.List;

/**
 * A registry that assembles and caches Inference Endpoints, {@link Model}, for reuse.
 * Models are high read and minimally written, where changes only occur during updates and deletes.
 * The cache is invalidated via the {@link ClearInferenceEndpointCacheAction} so that every node gets the invalidation
 * message.
 */
public class InferenceEndpointRegistry {

    private static final Setting<Boolean> INFERENCE_ENDPOINT_CACHE_ENABLED = Setting.boolSetting(
        "xpack.inference.endpoint.cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Setting<Integer> INFERENCE_ENDPOINT_CACHE_WEIGHT = Setting.intSetting(
        "xpack.inference.endpoint.cache.weight",
        25,
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
    private static final Cache.Stats EMPTY = new Cache.Stats(0, 0, 0);
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final ProjectResolver projectResolver;
    private final Cache<InferenceIdAndProject, Model> cache;
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
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.projectResolver = projectResolver;
        this.cache = CacheBuilder.<InferenceIdAndProject, Model>builder()
            .setMaximumWeight(INFERENCE_ENDPOINT_CACHE_WEIGHT.get(settings))
            .setExpireAfterWrite(INFERENCE_ENDPOINT_CACHE_EXPIRY.get(settings))
            .build();
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.cacheEnabledViaSetting = INFERENCE_ENDPOINT_CACHE_ENABLED.get(settings);

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(INFERENCE_ENDPOINT_CACHE_ENABLED, enabled -> this.cacheEnabledViaSetting = enabled);
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
                if (cacheKeys.next().projectId.equals(projectId)) {
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

            var model = service.parsePersistedConfigWithSecrets(
                unparsedModel.inferenceEntityId(),
                unparsedModel.taskType(),
                unparsedModel.settings(),
                unparsedModel.secrets()
            );

            if (cacheEnabled()) {
                cache.put(idAndProject, model);
            }
            l.onResponse(model);
        }));
    }

    public Cache.Stats stats() {
        return cacheEnabled() ? cache.stats() : EMPTY;
    }

    public int cacheCount() {
        return cacheEnabled() ? cache.count() : 0;
    }

    public boolean cacheEnabled() {
        return cacheEnabledViaSetting && cacheEnabledViaFeature();
    }

    private boolean cacheEnabledViaFeature() {
        var state = clusterService.state();
        return state.clusterRecovered() && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_ENDPOINT_CACHE);
    }

    private record InferenceIdAndProject(String inferenceEntityId, ProjectId projectId) {}
}
