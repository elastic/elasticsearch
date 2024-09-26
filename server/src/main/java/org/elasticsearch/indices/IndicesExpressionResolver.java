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
import org.elasticsearch.common.Strings;

import java.util.Map;

/**
 * Interface for resolving index expressions, along with IndicesOptions.
 * Implementations should support the following:
 * - plain index names
 * - cluster:index notation
 * - date math expression, including date math prefixed by a clusterAlias
 * - multiple index expressions (e.g.,  logs1,logs2,cluster-a:logs1
 *
 * How wildcards are handled could be implementation dependent.
 */
public interface IndicesExpressionResolver {

    /**
     * @param indicesOptions IndicesOptions to clarify how the index expression should be parsed/applied
     * @param indexExpressionCsv Multiple index expressions as CSV string (with no spaces), e.g., "logs1,logs2,cluster-a:logs1".
     *                           A single index expression is also supported
     * @return Map where the key is the cluster alias (for "local" cluster, it is RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)
     *         and the value is the resolved indices for that cluster from the index expression as an OriginalIndices object.
     */
    default Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String indexExpressionCsv) {
        return groupIndices(indicesOptions, Strings.splitStringByCommaToArray(indexExpressionCsv));
    }

    /**
     * Same behavior as the other groupIndices, except the incoming multiple index expressions must already be
     * parsed into a String array.
     * @param indicesOptions IndicesOptions to clarify how the index expression should be parsed/applied
     * @param indexExpressions Multiple index expressions as string[].
     * @return Map where the key is the cluster alias (for "local" cluster, it is RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)
     *         and the value is the resolved indices for that cluster from the index expression as an OriginalIndices object.
     */
    Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indexExpressions);
}
