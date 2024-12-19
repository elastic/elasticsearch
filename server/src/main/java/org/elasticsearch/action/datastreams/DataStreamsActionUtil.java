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

import java.util.List;
import java.util.SortedMap;
import java.util.stream.Stream;

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

    public static Stream<String> resolveConcreteIndexNames(
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

        return abstractionNames.stream().flatMap(abstractionName -> {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName.resource());
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                return selectDataStreamIndicesNames(
                    (DataStream) indexAbstraction,
                    IndexComponentSelector.FAILURES.equals(abstractionName.selector())
                );
            } else {
                return Stream.empty();
            }
        });
    }

    /**
     * Resolves a list of expressions into data stream names and then collects the concrete indices
     * that are applicable for those data streams based on the given selector.
     * @param indexNameExpressionResolver resolver object
     * @param clusterState state to query
     * @param names data stream expressions
     * @param selector which component indices of the data stream should be returned
     * @param indicesOptions options for expression resolution
     * @return A stream of concrete index names that belong to the components specified
     *         on the data streams returned from the expressions given
     */
    public static Stream<String> resolveConcreteIndexNamesWithSelector(
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

        return abstractionNames.stream().flatMap(abstractionName -> {
            IndexAbstraction indexAbstraction = indicesLookup.get(abstractionName);
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                Stream<String> backingIndices = null;
                Stream<String> failureIndices = null;

                if (selector.shouldIncludeData()) {
                    backingIndices = selectDataStreamIndicesNames((DataStream) indexAbstraction, false);
                }
                if (selector.shouldIncludeFailures()) {
                    failureIndices = selectDataStreamIndicesNames((DataStream) indexAbstraction, true);
                }

                assert backingIndices != null || failureIndices != null : "Could not resolve any indices for data stream";

                if (backingIndices == null) {
                    return failureIndices;
                } else if (failureIndices == null) {
                    return backingIndices;
                } else {
                    return Stream.concat(backingIndices, failureIndices);
                }
            } else {
                return Stream.empty();
            }
        });
    }

    private static Stream<String> selectDataStreamIndicesNames(DataStream indexAbstraction, boolean failureStore) {
        DataStream.DataStreamIndices dataStreamIndices = indexAbstraction.getDataStreamIndices(failureStore);
        List<Index> indices = dataStreamIndices.getIndices();
        return indices.stream().map(Index::getName);
    }
}
