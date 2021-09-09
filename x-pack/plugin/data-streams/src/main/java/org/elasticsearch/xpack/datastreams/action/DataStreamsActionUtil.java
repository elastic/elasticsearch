/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.EnumSet;
import java.util.List;

public class DataStreamsActionUtil {

    /**
     * Gets data streams names, expanding wildcards using {@link IndicesOptions} provided.
     * For data streams we only care for {@link IndicesOptions.WildcardStates}.HIDDEN state (we can't have closed or open data streams),
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
        EnumSet<IndicesOptions.WildcardStates> expandWildcards = indicesOptions.getExpandWildcards();
        expandWildcards.add(IndicesOptions.WildcardStates.OPEN);
        indicesOptions = new IndicesOptions(indicesOptions.getOptions(), expandWildcards);
        return indicesOptions;
    }
}
