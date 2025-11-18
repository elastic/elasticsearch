/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.common.BroadcastMessageAction;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Cache for whether CCM is enabled or disabled for this cluster as well as what the CCM key is for when it is enabled.
 */
public class CCMCache {

    private static final Setting<Integer> INFERENCE_CCM_CACHE_WEIGHT = Setting.intSetting(
        "xpack.inference.ccm.cache.weight",
        1,
        Setting.Property.NodeScope
    );

    private static final Setting<TimeValue> INFERENCE_CCM_CACHE_EXPIRY = Setting.timeSetting(
        "xpack.inference.ccm.cache.expiry_time",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope
    );

    public static Collection<? extends Setting<?>> getSettingsDefinitions() {
        return List.of(INFERENCE_CCM_CACHE_WEIGHT, INFERENCE_CCM_CACHE_EXPIRY);
    }

    private static final Logger logger = LogManager.getLogger(CCMCache.class);
    private static final Cache.Stats EMPTY = new Cache.Stats(0, 0, 0);
    private final CCMPersistentStorageService ccmPersistentStorageService;
    private final Cache<ProjectId, CCMModelEntry> cache;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final ProjectResolver projectResolver;
    private final Client client;

    public CCMCache(
        CCMPersistentStorageService ccmPersistentStorageService,
        ClusterService clusterService,
        Settings settings,
        FeatureService featureService,
        ProjectResolver projectResolver,
        Client client
    ) {
        this.ccmPersistentStorageService = ccmPersistentStorageService;
        this.cache = CacheBuilder.<ProjectId, CCMModelEntry>builder()
            .setMaximumWeight(INFERENCE_CCM_CACHE_WEIGHT.get(settings))
            .setExpireAfterWrite(INFERENCE_CCM_CACHE_EXPIRY.get(settings))
            .build();
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.projectResolver = projectResolver;
        this.client = client;
    }

    /**
     * Immediately returns the CCM key if it is cached, or goes to the index if there is no value cached or the previous call returned
     * nothing. The expectation is that the caller checks if CCM is enabled via the {@link #isEnabled(ActionListener)} API, which caches
     * a boolean value if the CCM key is present or absent in the underlying index.
     */
    public void get(ActionListener<CCMModel> listener) {
        var projectId = projectResolver.getProjectId();
        var cachedEntry = getCacheEntry(projectId);
        if (cachedEntry != null && cachedEntry.enabled()) {
            listener.onResponse(cachedEntry.ccmModel());
        } else {
            ccmPersistentStorageService.get(ActionListener.wrap(ccmModel -> {
                putEnabledEntry(projectId, ccmModel);
                listener.onResponse(ccmModel);
            }, e -> {
                if (e instanceof ResourceNotFoundException) {
                    putDisabledEntry(projectId);
                }
                listener.onFailure(e);
            }));
        }
    }

    private CCMModelEntry getCacheEntry(ProjectId projectId) {
        return cacheEnabled() ? cache.get(projectId) : null;
    }

    private boolean cacheEnabled() {
        var state = clusterService.state();
        return state.clusterRecovered() && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_CCM_CACHE);
    }

    private void putEnabledEntry(ProjectId projectId, CCMModel ccmModel) {
        if (cacheEnabled()) {
            cache.put(projectId, CCMModelEntry.enabled(ccmModel));
        }
    }

    private void putDisabledEntry(ProjectId projectId) {
        if (cacheEnabled()) {
            cache.put(projectId, CCMModelEntry.DISABLED);
        }
    }

    /**
     * Checks if the value is present or absent based on a previous call to {@link #isEnabled(ActionListener)}
     * or {@link #get(ActionListener)}. If the cache entry is missing or expired, then it will call through to the backing index.
     */
    public void isEnabled(ActionListener<Boolean> listener) {
        var projectId = projectResolver.getProjectId();
        var cachedEntry = getCacheEntry(projectId);
        if (cachedEntry != null) {
            listener.onResponse(cachedEntry.enabled());
        } else {
            ccmPersistentStorageService.get(ActionListener.wrap(ccmModel -> {
                putEnabledEntry(projectId, ccmModel);
                listener.onResponse(true);
            }, e -> {
                if (e instanceof ResourceNotFoundException) {
                    putDisabledEntry(projectId);
                    listener.onResponse(false);
                } else {
                    listener.onFailure(e);
                }
            }));
        }
    }

    public void invalidate(ActionListener<Void> listener) {
        if (cacheEnabled()) {
            client.execute(
                ClearCCMCacheAction.INSTANCE,
                ClearCCMCacheAction.request(ClearCCMMessage.INSTANCE, null),
                ActionListener.wrap(ack -> {
                    logger.debug("Successfully refreshed inference CCM cache for project {}.", projectResolver::getProjectId);
                    listener.onResponse((Void) null);
                }, e -> {
                    logger.atDebug()
                        .withThrowable(e)
                        .log("Failed to refresh inference CCM cache for project {}.", projectResolver::getProjectId);
                    listener.onFailure(e);
                })
            );
        }
    }

    private void invalidate(ProjectId projectId) {
        if (cacheEnabled()) {
            var cacheKeys = cache.keys().iterator();
            while (cacheKeys.hasNext()) {
                if (cacheKeys.next().equals(projectId)) {
                    cacheKeys.remove();
                }
            }
        }
    }

    public Cache.Stats stats() {
        return cacheEnabled() ? cache.stats() : EMPTY;
    }

    public int cacheCount() {
        return cacheEnabled() ? cache.count() : 0;
    }

    private record CCMModelEntry(boolean enabled, @Nullable CCMModel ccmModel) {
        private static final CCMModelEntry DISABLED = new CCMModelEntry(false, null);

        private static CCMModelEntry enabled(CCMModel ccmModel) {
            return new CCMModelEntry(true, Objects.requireNonNull(ccmModel));
        }
    }

    public static class ClearCCMCacheAction extends BroadcastMessageAction<ClearCCMMessage> {
        private static final String NAME = "cluster:internal/xpack/inference/clear_inference_ccm_cache";
        public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

        private final ProjectResolver projectResolver;
        private final CCMCache ccmCache;

        @Inject
        public ClearCCMCacheAction(
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            CCMCache ccmCache
        ) {
            super(NAME, clusterService, transportService, actionFilters, in -> ClearCCMMessage.INSTANCE);

            this.projectResolver = projectResolver;
            this.ccmCache = ccmCache;
        }

        @Override
        protected void receiveMessage(ClearCCMMessage clearCCMMessage) {
            ccmCache.invalidate(projectResolver.getProjectId());
        }
    }

    public record ClearCCMMessage() implements Writeable {
        private static final ClearCCMMessage INSTANCE = new ClearCCMMessage();

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
