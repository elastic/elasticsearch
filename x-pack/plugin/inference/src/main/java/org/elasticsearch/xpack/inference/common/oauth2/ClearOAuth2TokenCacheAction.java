/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.inference.common.BroadcastMessageAction;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

import java.io.IOException;

/**
 * Broadcasts a cache-invalidation event for a single {@link InferenceIdAndProject} key to every
 * node in the cluster. Each node removes the matching entry from its local {@link OAuth2TokenCache}.
 *
 * <p>This action uses the {@link BroadcastMessageAction} pattern (fire-and-forget transport) to tell other nodes to clear their cache.
 * This is what {@link org.elasticsearch.xpack.inference.services.elastic.ccm.CCMCache.ClearCCMCacheAction} does as well.
 *
 * <p>The message carries the full {@link InferenceIdAndProject} key because per-endpoint
 * invalidation must target a specific {@code (inferenceId, projectId)} pair.
 */
public class ClearOAuth2TokenCacheAction extends BroadcastMessageAction<ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage> {

    private static final String NAME = "cluster:internal/xpack/inference/clear_oauth2_token_cache";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private final OAuth2TokenCache cache;

    @Inject
    public ClearOAuth2TokenCacheAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        OAuth2TokenCache cache
    ) {
        super(NAME, clusterService, transportService, actionFilters, ClearOAuth2TokenMessage::new);
        this.cache = cache;
    }

    @Override
    public void receiveMessage(ClearOAuth2TokenMessage message) {
        cache.invalidateLocal(message.key());
    }

    /**
     * The wire payload carrying the cache key to invalidate on each receiving node.
     */
    public record ClearOAuth2TokenMessage(InferenceIdAndProject key) implements Writeable {

        public ClearOAuth2TokenMessage(StreamInput in) throws IOException {
            this(new InferenceIdAndProject(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
        }
    }
}
