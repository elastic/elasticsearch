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
 * Extension point for supplying the {@code tags} configuration snapshot (tag names, named routing expressions, etc.)
 * to {@code GET _cluster/stats}. Registered via SPI ({@code META-INF/services/}) and loaded by
 * {@code NodeConstruction} using {@code loadSingletonServiceProvider}.
 *
 * <p>Needed as an extension point for serverless code.
 */
@FunctionalInterface
public interface ClusterStatsTagsProvider {

    /**
     * Returns the current tags configuration snapshot for the request's project, or {@code null} if the configuration
     * is unavailable (e.g. CPS is disabled, or the project cannot be resolved from the thread context).
     */
    @Nullable
    TagsConfigSnapshot getTagsConfig(ClusterState clusterState);
}
