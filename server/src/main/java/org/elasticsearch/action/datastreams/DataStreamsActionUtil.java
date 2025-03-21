/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.WildcardOptions;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
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
        ProjectMetadata project,
        String[] names,
        IndicesOptions indicesOptions
    ) {
        indicesOptions = updateIndicesOptions(indicesOptions);
        return indexNameExpressionResolver.dataStreamNames(project, indicesOptions, names);
    }

    public static IndicesOptions updateIndicesOptions(IndicesOptions indicesOptions) {
        // if expandWildcardsOpen=false, then it will be overridden to true
        if (indicesOptions.expandWildcardsOpen() == false) {
            indicesOptions = IndicesOptions.builder(indicesOptions)
                .wildcardOptions(WildcardOptions.builder(indicesOptions.wildcardOptions()).matchOpen(true))
                .build();
        }
        return indicesOptions;
    }

    public static List<String> resolveConcreteIndexNames(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectMetadata project,
        String[] names,
        IndicesOptions indicesOptions
    ) {
        List<ResolvedExpression> resolvedDataStreamExpressions = indexNameExpressionResolver.dataStreams(
            project,
            updateIndicesOptions(indicesOptions),
            names
        );
        SortedMap<String, IndexAbstraction> indicesLookup = project.getIndicesLookup();

        List<String> results = new ArrayList<>(resolvedDataStreamExpressions.size());
        for (ResolvedExpression resolvedExpression : resolvedDataStreamExpressions) {
            IndexAbstraction indexAbstraction = indicesLookup.get(resolvedExpression.resource());
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                DataStream dataStream = (DataStream) indexAbstraction;
                if (IndexNameExpressionResolver.shouldIncludeRegularIndices(indicesOptions, resolvedExpression.selector())) {
                    selectDataStreamIndicesNames(dataStream, false, results);
                }
                if (IndexNameExpressionResolver.shouldIncludeFailureIndices(indicesOptions, resolvedExpression.selector())) {
                    selectDataStreamIndicesNames(dataStream, true, results);
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
