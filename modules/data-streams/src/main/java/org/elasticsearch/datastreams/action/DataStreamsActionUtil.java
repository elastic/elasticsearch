/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.List;

public class DataStreamsActionUtil {

    /**
     * Gets data streams names, expanding wildcards using {@link IndicesOptions} provided.
     * For data streams we only care about the hidden state (we can't have closed or open data streams),
     * but we have to have either OPEN or CLOSE to have any names returned from {@link IndexNameExpressionResolver}. So here we always
     * add OPEN to make sure that happens.
     */
    public static List<String> getDataStreamNames(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState currentState,
        String[] names,
        IndicesOptions indicesOptions
    ) {
        indicesOptions = updateIndicesOptions(indicesOptions);
        return indexNameExpressionResolver.dataStreamNames(currentState, indicesOptions, names);
    }

    public static IndicesOptions updateIndicesOptions(IndicesOptions indicesOptions) {
        if (indicesOptions.expandWildcardsOpen() == false) {
            indicesOptions = IndicesOptions.builder(indicesOptions)
                .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).matchOpen(true))
                .build();
        }
        return indicesOptions;
    }
}
