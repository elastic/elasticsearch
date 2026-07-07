/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.GetRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.Objects;

/**
 * Caches {@link InferencePreferences} per project so they are readily available when building outgoing
 * Elastic Inference Service requests, without a synchronous read of the {@code .inference} index on every
 * request. Populated lazily on a cache miss; invalidated cluster-wide via
 * {@link ClearInferencePreferencesCacheAction} whenever the region policy is written or deleted.
 *
 * <p>Gated by {@link InferenceFeatures#INFERENCE_CLEAR_PREFERENCES_CACHE}: if the cluster doesn't have that
 * feature (e.g. an old node hasn't upgraded yet), caching is disabled entirely — {@link #get} always fetches,
 * and {@link #invalidate} is a no-op instead of broadcasting, since there's nothing cached to clear.
 */
public class InferencePreferencesCache {

    private static final Logger logger = LogManager.getLogger(InferencePreferencesCache.class);
    // Reasonable default max cache size for when multiple projects are hosted in the same cluster
    private static final int CACHE_MAX_WEIGHT = 100;
    private static final TimeValue CACHE_TTL = TimeValue.timeValueMinutes(15);

    /**
     * Fetches the current project's region policy, or {@code null} if none is configured. Extracted as a seam
     * so cache mechanics can be unit-tested without executing {@link GetRegionPolicyAction}. Public because
     * tests that need to inject a fetcher (e.g. {@code ElasticInferenceServiceActionCreatorTests}) live outside
     * this class's package.
     */
    @FunctionalInterface
    public interface RegionPolicyFetcher {
        void fetch(ActionListener<RegionPolicy> listener);
    }

    private final Cache<ProjectId, InferencePreferences> cache = CacheBuilder.<ProjectId, InferencePreferences>builder()
        .setMaximumWeight(CACHE_MAX_WEIGHT)
        .setExpireAfterWrite(CACHE_TTL)
        .build();
    private final ProjectResolver projectResolver;
    private final OriginSettingClient client;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final RegionPolicyFetcher regionPolicyFetcher;

    public InferencePreferencesCache(
        ProjectResolver projectResolver,
        Client client,
        ClusterService clusterService,
        FeatureService featureService
    ) {
        this.projectResolver = Objects.requireNonNull(projectResolver);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
        this.regionPolicyFetcher = this::fetchRegionPolicy;
    }

    // Visible for testing: injects a fetcher so cache mechanics can be tested without executing GetRegionPolicyAction.
    public InferencePreferencesCache(
        ProjectResolver projectResolver,
        Client client,
        ClusterService clusterService,
        FeatureService featureService,
        RegionPolicyFetcher regionPolicyFetcher
    ) {
        this.projectResolver = Objects.requireNonNull(projectResolver);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
        this.regionPolicyFetcher = Objects.requireNonNull(regionPolicyFetcher);
    }

    private boolean cacheEnabled() {
        var state = clusterService.state();
        return state.clusterRecovered() && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_CLEAR_PREFERENCES_CACHE);
    }

    private void fetchRegionPolicy(ActionListener<RegionPolicy> listener) {
        if (InferencePlugin.INFERENCE_REGION_POLICY_FEATURE_FLAG.isEnabled() == false) {
            listener.onResponse(null);
            return;
        }
        client.execute(
            GetRegionPolicyAction.INSTANCE,
            new GetRegionPolicyAction.Request(),
            listener.delegateFailureAndWrap((delegate, response) -> delegate.onResponse(response.regionPolicy().regionPolicy()))
        );
    }

    /**
     * Returns the cached preferences for the current project, fetching from the {@code .inference} index on a
     * cache miss. If no region policy is configured, resolves to {@link InferencePreferences#EMPTY}. If fetching
     * fails for any other reason, fails the listener instead of silently proceeding without preferences.
     */
    public void get(ActionListener<InferencePreferences> listener) {
        final var projectId = projectResolver.getProjectId();
        final var cacheEnabled = cacheEnabled();
        final var cached = cacheEnabled ? cache.get(projectId) : null;
        if (cached != null) {
            listener.onResponse(cached);
            return;
        }

        regionPolicyFetcher.fetch(ActionListener.wrap(regionPolicy -> {
            var preferences = new InferencePreferences(regionPolicy);
            if (cacheEnabled) {
                cache.put(projectId, preferences);
            }
            listener.onResponse(preferences);
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                if (cacheEnabled) {
                    cache.put(projectId, InferencePreferences.EMPTY);
                }
                listener.onResponse(InferencePreferences.EMPTY);
                return;
            }
            logger.warn(() -> "Failed to fetch inference preferences for project [" + projectId + "]", e);
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Failed to fetch inference preferences for project [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e,
                    projectId
                )
            );
        }));
    }

    /**
     * Invalidates the cached entry for the current project across the entire cluster. A no-op if caching is
     * disabled, since there's nothing cached to clear.
     */
    public void invalidate(ActionListener<Void> listener) {
        if (cacheEnabled() == false) {
            listener.onResponse(null);
            return;
        }
        client.execute(
            ClearInferencePreferencesCacheAction.INSTANCE,
            ClearInferencePreferencesCacheAction.request(
                ClearInferencePreferencesCacheAction.ClearInferencePreferencesMessage.INSTANCE,
                null
            ),
            ActionListener.wrap(ack -> listener.onResponse(null), listener::onFailure)
        );
    }

    /**
     * Removes the cached entry for the current project on this node only. Invoked when this node receives a
     * broadcast {@link ClearInferencePreferencesCacheAction} message.
     */
    void invalidateLocal() {
        cache.invalidate(projectResolver.getProjectId());
    }

    public Cache.Stats stats() {
        return cache.stats();
    }
}
