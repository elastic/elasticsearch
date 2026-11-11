/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Broadcasts a cache-invalidation event to every node in the cluster. Each node removes the current
 * project's entry from its local {@link InferencePreferencesCache}.
 *
 * <p>Uses the {@link BroadcastMessageAction} pattern (fire-and-forget transport), mirroring
 * {@code org.elasticsearch.xpack.inference.services.elastic.ccm.CCMCache.ClearCCMCacheAction}.
 */
public class ClearInferencePreferencesCacheAction extends BroadcastMessageAction<
    ClearInferencePreferencesCacheAction.ClearInferencePreferencesMessage> {

    private static final String NAME = "cluster:internal/xpack/inference/clear_inference_preferences_cache";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private final InferencePreferencesCache cache;

    @Inject
    public ClearInferencePreferencesCacheAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        InferencePreferencesCache cache
    ) {
        super(NAME, clusterService, transportService, actionFilters, in -> ClearInferencePreferencesMessage.INSTANCE);
        this.cache = cache;
    }

    @Override
    protected void receiveMessage(ClearInferencePreferencesMessage message) {
        cache.invalidateLocal();
    }

    /**
     * Empty message: the project to invalidate is resolved locally on each receiving node via
     * {@link InferencePreferencesCache}'s own {@code ProjectResolver}.
     */
    public record ClearInferencePreferencesMessage() implements Writeable {
        public static final ClearInferencePreferencesMessage INSTANCE = new ClearInferencePreferencesMessage();

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
