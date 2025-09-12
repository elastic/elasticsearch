/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ReplacedIndexExpressions;
import org.elasticsearch.action.ResponseWithReplacedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Service for managing cross-project search operations.
 */
public interface CrossProjectSearchService {
    /**
     * Factory for creating instances of the CrossProjectSearchService. Required for SPI no-args constructor.
     */
    interface Factory {
        CrossProjectSearchService create(ThreadContext threadContext, Settings settings, ClusterSettings clusterSettings);

        Factory DEFAULT = (ignored, ignored2, ignored3) -> new Default();
    }

    /**
     * Default no-op implementation of the CrossProjectSearchService.
     */
    class Default implements CrossProjectSearchService {
        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public boolean crossProjectContextActive() {
            return false;
        }

        @Override
        public IndicesRequest.CrossProjectSearchCapable maybeRewriteRequest(IndicesRequest.CrossProjectSearchCapable request) {
            return null;
        }

        @Override
        public IndicesOptions fanoutIndicesOptions(IndicesOptions indicesOptions) {
            return indicesOptions;
        }

        @Override
        public void crossProjectFanoutErrorHandling(
            IndicesOptions indicesOptions,
            ReplacedIndexExpressions originReplacedIndexExpressions,
            Map<String, ? extends ResponseWithReplacedIndexExpressions> remoteResults
        ) {}
    }

    /**
     * Is cross-project search enabled on this cluster?
     */
    boolean enabled();

    /**
     * Is the current request running in a cross-project context?
     * Note: this may be false, even if {@link #enabled()} is true.
     * <p>
     * TODO should this consume a thread context for additional clarity?
     */
    boolean crossProjectContextActive();

    // Index expression rewrite logic (could be a separate class/interface)

    @Nullable
    IndicesRequest.CrossProjectSearchCapable maybeRewriteRequest(IndicesRequest.CrossProjectSearchCapable request);

    // Remote fanout logic (could be a separate class/interface)

    IndicesOptions fanoutIndicesOptions(IndicesOptions indicesOptions);

    void crossProjectFanoutErrorHandling(
        IndicesOptions indexOptions,
        ReplacedIndexExpressions originReplacedIndexExpressions,
        Map<String, ? extends ResponseWithReplacedIndexExpressions> remoteResults
    );
}
