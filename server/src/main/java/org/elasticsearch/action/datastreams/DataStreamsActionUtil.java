/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

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

    public static List<String> resolveConcreteIndexNames(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        String[] names,
        IndicesOptions indicesOptions
    ) {
        List<ResolvedExpression> abstractionNames = indexNameExpressionResolver.dataStreams(
            clusterState,
            updateIndicesOptions(indicesOptions),
            names
        );
        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();

        List<String> results = new ArrayList<>(abstractionNames.size());
        for (ResolvedExpression abstractionName : abstractionNames) {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName.resource());
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                selectDataStreamIndicesNames(
                    (DataStream) indexAbstraction,
                    IndexComponentSelector.FAILURES.equals(abstractionName.selector()),
                    results
                );
            }
        }
        return results;
    }

    /**
     * Resolves a list of expressions into data stream names and then collects the concrete indices
     * that are applicable for those data streams based on the selector provided in the arguments.
     * @param indexNameExpressionResolver resolver object
     * @param clusterState state to query
     * @param names data stream expressions
     * @param selector which component indices of the data stream should be returned
     * @param indicesOptions options for expression resolution
     * @return A stream of concrete index names that belong to the components specified
     *         on the data streams returned from the expressions given
     */
    public static List<String> resolveConcreteIndexNamesWithSelector(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        String[] names,
        IndexComponentSelector selector,
        IndicesOptions indicesOptions
    ) {
        assert indicesOptions.allowSelectors() == false : "If selectors are enabled, use resolveConcreteIndexNames instead";
        List<String> abstractionNames = indexNameExpressionResolver.dataStreamNames(
            clusterState,
            updateIndicesOptions(indicesOptions),
            names
        );
        SortedMap<String, IndexAbstraction> indicesLookup = clusterState.getMetadata().getIndicesLookup();

        List<String> results = new ArrayList<>(abstractionNames.size());
        for (String abstractionName : abstractionNames) {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName);
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                if (selector.shouldIncludeData()) {
                    selectDataStreamIndicesNames((DataStream) indexAbstraction, false, results);
                }
                if (selector.shouldIncludeFailures()) {
                    selectDataStreamIndicesNames((DataStream) indexAbstraction, true, results);
                }
            }
        }
        return results;
    }

    private static void selectDataStreamIndicesNames(DataStream indexAbstraction, boolean failureStore, List<String> accumulator) {
        for (Index index : indexAbstraction.getDataStreamIndices(failureStore).getIndices()) {
            accumulator.add(index.getName());
        }
    }
}
