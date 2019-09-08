/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class contains more complex validations in regards to how {@link DataFrameTransformConfig#getSource()} and
 * {@link DataFrameTransformConfig#getDestination()} relate to each other.
 */
public final class SourceDestValidator {

    interface SourceDestValidation {
        boolean isDeferrable();
        void validate(DataFrameTransformConfig config, ClusterState clusterState, IndexNameExpressionResolver indexNameExpressionResolver);
    }

    private static final List<SourceDestValidation> VALIDATIONS = Arrays.asList(new SourceMissingValidation(),
        new DestinationInSourceValidation(),
        new DestinationSingleIndexValidation());

    /**
     * Validates the DataFrameTransformConfiguration source and destination indices.
     *
     * A simple name validation is done on {@link DataFrameTransformConfig#getDestination()} inside
     * {@link org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction}
     *
     * So, no need to do the name checks here.
     *
     * @param config DataFrameTransformConfig to validate
     * @param clusterState The current ClusterState
     * @param indexNameExpressionResolver A valid IndexNameExpressionResolver object
     * @throws ElasticsearchStatusException when a validation fails
     */
    public static void validate(DataFrameTransformConfig config,
                                ClusterState clusterState,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                boolean shouldDefer) {
        for (SourceDestValidation validation : VALIDATIONS) {
            if (shouldDefer && validation.isDeferrable()) {
                continue;
            }
            validation.validate(config, clusterState, indexNameExpressionResolver);
        }
    }

    static class SourceMissingValidation implements SourceDestValidation {

        @Override
        public boolean isDeferrable() {
            return true;
        }

        @Override
        public void validate(DataFrameTransformConfig config,
                             ClusterState clusterState,
                             IndexNameExpressionResolver indexNameExpressionResolver) {
            for(String src : config.getSource().getIndex()) {
                String[] concreteNames = indexNameExpressionResolver.concreteIndexNames(clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    src);
                if (concreteNames.length == 0) {
                    throw new ElasticsearchStatusException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_SOURCE_INDEX_MISSING, src),
                        RestStatus.BAD_REQUEST);
                }
            }
        }
    }

    static class DestinationInSourceValidation implements SourceDestValidation {

        @Override
        public boolean isDeferrable() {
            return true;
        }

        @Override
        public void validate(DataFrameTransformConfig config,
                             ClusterState clusterState,
                             IndexNameExpressionResolver indexNameExpressionResolver) {
            final String destIndex = config.getDestination().getIndex();
            Set<String> concreteSourceIndexNames = new HashSet<>();
            for(String src : config.getSource().getIndex()) {
                String[] concreteNames = indexNameExpressionResolver.concreteIndexNames(clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    src);
                if (Regex.simpleMatch(src, destIndex)) {
                    throw new ElasticsearchStatusException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_IN_SOURCE, destIndex, src),
                        RestStatus.BAD_REQUEST);
                }
                concreteSourceIndexNames.addAll(Arrays.asList(concreteNames));
            }

            if (concreteSourceIndexNames.contains(destIndex)) {
                throw new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_IN_SOURCE,
                        destIndex,
                        Strings.arrayToCommaDelimitedString(config.getSource().getIndex())),
                    RestStatus.BAD_REQUEST
                );
            }

            final String[] concreteDest = indexNameExpressionResolver.concreteIndexNames(clusterState,
                IndicesOptions.lenientExpandOpen(),
                destIndex);
            if (concreteDest.length > 0 && concreteSourceIndexNames.contains(concreteDest[0])) {
                throw new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_IN_SOURCE,
                        concreteDest[0],
                        Strings.arrayToCommaDelimitedString(concreteSourceIndexNames.toArray(new String[0]))),
                    RestStatus.BAD_REQUEST
                );
            }
        }
    }

    static class DestinationSingleIndexValidation implements SourceDestValidation {

        @Override
        public boolean isDeferrable() {
            return false;
        }

        @Override
        public void validate(DataFrameTransformConfig config,
                             ClusterState clusterState,
                             IndexNameExpressionResolver indexNameExpressionResolver) {
            final String destIndex = config.getDestination().getIndex();
            final String[] concreteDest =
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), destIndex);

            if (concreteDest.length > 1) {
                throw new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_SINGLE_INDEX, destIndex),
                    RestStatus.BAD_REQUEST
                );
            }
        }
    }
}
