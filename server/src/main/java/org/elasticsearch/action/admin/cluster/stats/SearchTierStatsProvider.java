/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Nullable;

/**
 * Extension point for supplying the {@code search_tier} section of {@code GET _cluster/stats}. Registered via SPI
 * ({@code META-INF/services/}) and loaded by {@code NodeConstruction} using {@code loadSingletonServiceProvider}.
 * When no provider is registered the section is omitted.
 *
 * <p>Needed as an extension point for serverless code, since the search tier only exists there.
 */
@FunctionalInterface
public interface SearchTierStatsProvider {

    /**
     * Returns the current search tier stats, or {@code null} if the {@code search_tier} section should be omitted
     * from the response (e.g. the project type has no search tier).
     */
    @Nullable
    SearchTierStats getSearchTierStats(ClusterState clusterState);
}
