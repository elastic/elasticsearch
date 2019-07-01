/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class SourceDestValidator {

    private final ClusterState clusterState;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public SourceDestValidator(ClusterState clusterState, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterState = Objects.requireNonNull(clusterState);
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
    }

    public void check(DataFrameAnalyticsConfig config) {
        String[] sourceIndex = config.getSource().getIndex();
        String destIndex = config.getDest().getIndex();

        String[] sourceExpressions = Arrays.stream(sourceIndex)
            .map(index -> Strings.tokenizeToStringArray(index, ","))
            .flatMap(Arrays::stream)
            .toArray(String[]::new);

        for (String sourceExpression : sourceExpressions) {
            if (Regex.simpleMatch(sourceExpression, destIndex)) {
                throw ExceptionsHelper.badRequestException("Destination index [{}] must not be included in source index [{}]",
                    destIndex, sourceExpression);
            }
        }

        Set<String> concreteSourceIndexNames = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(), sourceExpressions)));

        if (concreteSourceIndexNames.isEmpty()) {
            throw ExceptionsHelper.badRequestException("No index matches source index {}", Arrays.toString(sourceIndex));
        }

        final String[] concreteDestIndexNames = indexNameExpressionResolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(), destIndex);

        if (concreteDestIndexNames.length > 1) {
            // In case it is an alias, it may match multiple indices
            throw ExceptionsHelper.badRequestException("Destination index [{}] should match a single index; matches {}", destIndex,
                Arrays.toString(concreteDestIndexNames));
        }
        if (concreteDestIndexNames.length == 1 && concreteSourceIndexNames.contains(concreteDestIndexNames[0])) {
            // In case the dest index is an alias, we need to check the concrete index is not matched by source
            throw ExceptionsHelper.badRequestException("Destination index [{}], which is an alias for [{}], " +
                    "must not be included in source index {}", destIndex, concreteDestIndexNames[0], Arrays.toString(sourceIndex));
        }
    }
}
