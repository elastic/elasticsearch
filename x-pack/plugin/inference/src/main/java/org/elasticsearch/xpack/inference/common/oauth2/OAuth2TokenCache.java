/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.common.BroadcastMessageAction;
import org.elasticsearch.xpack.inference.common.DiagnosticsCache;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry.DEFAULT_CACHE_WEIGHT;

/**
 * A node-local cache for OAuth2 bearer tokens, keyed by {@link InferenceIdAndProject}.
 *
 * <p>Callers supply an {@link OAuth2TokenSupplier} that fetches a fresh token
 * from the Identity Provider (IdP) on cache miss. Concurrent callers for the same key are coalesced: only one fetch is in-flight
 * at a time, and all waiters receive the result together.
 *
 * <p>Tokens are checked against their {@code expiresAt} on every read with a 60-second skew
 * buffer. The {@code setExpireAfterWrite} backstop prevents indefinite memory accumulation if
 * the IdP returns very long-lived tokens.
 *
 * <p>Cluster-wide invalidation is handled by {@link ClearOAuth2TokenCacheAction}, which broadcasts
 * to every node so that secret or scope rotation takes effect before the natural token TTL.
 *
 * <p>Tokens are kept only in heap memory and are never persisted. Each node fetches its own
 * tokens independently.
 */
public class OAuth2TokenCache extends DiagnosticsCache<CachedToken> implements TokenCache {

    /**
     * 60-second buffer subtracted from a token's {@code expiresAt} when deciding whether a
     * cached entry is still usable. Prevents stale tokens being dispatched to an IdP endpoint
     * immediately before natural expiry.
     */
    static final Duration EXPIRY_SKEW = Duration.ofSeconds(60);

    static final Setting<Boolean> OAUTH2_TOKEN_CACHE_ENABLED = Setting.boolSetting(
        "xpack.inference.oauth2.token_cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(OAuth2TokenCache.class);

    /**
     * Weight here means the maximum number of tokens to cache. We use the same default weight as the
     * {@link org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry} because in the situation where every inference
     * endpoint has a unique token, we want the cache to be able to hold all of them. It could be useful to have the OAuth2 token cache
     * be larger than the endpoint cache since the tokens could live longer than the endpoint objects.
     */
    private static final Setting<Integer> OAUTH2_TOKEN_CACHE_WEIGHT = Setting.intSetting(
        "xpack.inference.oauth2.token_cache.weight",
        DEFAULT_CACHE_WEIGHT,
        Setting.Property.NodeScope
    );

    /**
     * Backstop expiry to prevent memory leaks in the case of very long-lived tokens.
     * <p>
     * For some examples:
     * <ul>
     *   <li>Google's tokens range from <a href="https://docs.cloud.google.com/docs/authentication/token-types#access-tokens">5 minutes and 12 hours</a></li>
     *   <li>Microsoft's tokens range from <a href="https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#token-lifetime">60 to 90 minutes</a></li>
     *   <li>AWS's tokens range from <a href="https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html">5 minutes and 1 day</a></li>
     * </ul>
     * Using a default of 1 hour here to strike a balance between accommodating long-lived tokens and ensuring that misconfigurations
     * don't lead to memory leaks.
     */
    private static final Setting<TimeValue> OAUTH2_TOKEN_CACHE_EXPIRY = Setting.timeSetting(
        "xpack.inference.oauth2.token_cache.expiry_time",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueHours(24),
        Setting.Property.NodeScope
    );

    public static Collection<? extends Setting<?>> getSettingsDefinitions() {
        return List.of(OAUTH2_TOKEN_CACHE_ENABLED, OAUTH2_TOKEN_CACHE_WEIGHT, OAUTH2_TOKEN_CACHE_EXPIRY);
    }

    private final ConcurrentHashMap<InferenceIdAndProject, SubscribableListener<CachedToken>> inflight = new ConcurrentHashMap<>();
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final Client client;
    private volatile boolean cacheEnabledViaSetting;

    public OAuth2TokenCache(
        ClusterService clusterService,
        Settings settings,
        FeatureService featureService,
        ProjectResolver projectResolver,
        Client client
    ) {
        super(buildCache(settings));
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.projectResolver = projectResolver;
        this.client = client;
        this.cacheEnabledViaSetting = OAUTH2_TOKEN_CACHE_ENABLED.get(settings);
    }

    private static Cache<InferenceIdAndProject, CachedToken> buildCache(Settings settings) {
        return CacheBuilder.<InferenceIdAndProject, CachedToken>builder()
            .setMaximumWeight(OAUTH2_TOKEN_CACHE_WEIGHT.get(settings))
            .setExpireAfterWrite(OAUTH2_TOKEN_CACHE_EXPIRY.get(settings))
            .removalListener(
                notification -> logger.debug(
                    "Token for inference id [{}] of project [{}] from OAuth2 token cache due to [{}].",
                    notification.getKey().inferenceEntityId(),
                    notification.getKey().projectId(),
                    notification.getRemovalReason()
                )
            )
            .build();
    }

    public void init() {
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(OAUTH2_TOKEN_CACHE_ENABLED, enabled -> cacheEnabledViaSetting = enabled);
    }

    /**
     * Returns a valid bearer token for the given inference endpoint, fetching one via
     * {@code supplier} if none is cached or the cached token is about to expire.
     *
     * <p>Concurrent calls for the same key are coalesced: only one supplier invocation runs at
     * a time, and all concurrent waiters attach to the same in-flight result.
     */
    public void getToken(String inferenceId, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener) {
        var key = new InferenceIdAndProject(inferenceId, projectResolver.getProjectId());

        if (cacheEnabled()) {
            var cached = cache.get(key);
            if (cached != null && cached.isExpiringSoon(EXPIRY_SKEW)) {
                // Token is expiring soon — treat as a miss so we proactively refresh
                cache.invalidate(key);
            } else if (cached != null) {
                listener.onResponse(cached);
                return;
            }
        }

        fetchWithCoalescing(key, supplier, listener);
    }

    private void fetchWithCoalescing(InferenceIdAndProject key, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener) {
        var newFuture = new SubscribableListener<CachedToken>();
        var existing = inflight.putIfAbsent(key, newFuture);
        var future = existing != null ? existing : newFuture;
        future.addListener(listener);

        if (existing == null) {
            // We created the future — start the fetch
            supplier.fetch(ActionListener.wrap(token -> {
                if (cacheEnabled()) {
                    cache.put(key, token);
                }
                inflight.remove(key, newFuture);
                newFuture.onResponse(token);
            }, e -> {
                inflight.remove(key, newFuture);
                newFuture.onFailure(e);
            }));
        }
    }

    /**
     * Broadcasts a cache-invalidation message to every node, causing each node to remove the
     * entry for {@code key} from its local cache. Used when OAuth2 settings are updated or the
     * inference endpoint is deleted.
     */
    public void invalidate(InferenceIdAndProject key, ActionListener<Void> listener) {
        if (cacheEnabled() == false) {
            listener.onResponse(null);
            return;
        }
        client.execute(
            ClearOAuth2TokenCacheAction.INSTANCE,
            BroadcastMessageAction.request(new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(key), null),
            ActionListener.wrap(ack -> {
                logger.debug("Successfully invalidated OAuth2 token cache for [{}].", key.inferenceEntityId());
                listener.onResponse(null);
            }, e -> {
                logger.warn(Strings.format("Failed to invalidate OAuth2 token cache for [%s].", key.inferenceEntityId()), e);
                listener.onFailure(e);
            })
        );
    }

    /**
     * Removes the entry for {@code key} from this node's local cache.
     * Called by {@link ClearOAuth2TokenCacheAction} on each node when a broadcast is received.
     */
    void invalidateLocal(InferenceIdAndProject key) {
        cache.invalidate(key);
    }

    @Override
    public boolean cacheEnabled() {
        return cacheEnabledViaSetting && cacheEnabledViaFeature();
    }

    private boolean cacheEnabledViaFeature() {
        var state = clusterService.state();
        return state.clusterRecovered() && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_OAUTH2_TOKEN_CACHE);
    }
}
