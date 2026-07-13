/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.Map;

/**
 * Interface for grouping index expressions, along with IndicesOptions by cluster alias.
 * Implementations should support the following:
 * - plain index names
 * - cluster:index notation
 * - date math expression, including date math prefixed by a clusterAlias
 * - wildcards
 * - multiple index expressions (e.g., logs1,logs2,cluster-a:logs*)
 *
 * Note: these methods do not resolve index expressions to concrete indices.
 */
public interface IndicesExpressionGrouper {

    /**
     * See {@link org.elasticsearch.transport.RemoteClusterService#groupIndices} for details
     */
    Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indexExpressions, boolean returnLocalAll);
}
