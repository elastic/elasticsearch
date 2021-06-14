/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataFrameAnalysis extends ToXContentObject, NamedWriteable {

    /**
     * @return The analysis parameters as a map
     * @param fieldInfo Information about the fields like types and cardinalities
     */
    Map<String, Object> getParams(FieldInfo fieldInfo);

    /**
     * @return {@code true} if this analysis supports fields with categorical values (i.e. text, keyword, ip)
     */
    boolean supportsCategoricalFields();

    /**
     * @param fieldName field for which the allowed categorical types should be returned
     * @return The types treated as categorical for the given field
     */
    Set<String> getAllowedCategoricalTypes(String fieldName);

    /**
     * @return The names and types of the fields that analyzed documents must have for the analysis to operate
     */
    List<RequiredField> getRequiredFields();

    /**
     * @return {@link List} containing cardinality constraints for the selected (analysis-specific) fields
     */
    List<FieldCardinalityConstraint> getFieldCardinalityConstraints();

    /**
     * Returns fields for which the mappings should be either predefined or copied from source index to destination index.
     *
     * @param resultsFieldName name of the results field under which all the results are stored
     * @param fieldCapabilitiesResponse field capabilities fetched for this analysis' required fields
     * @return {@link Map} containing fields for which the mappings should be handled explicitly
     */
    Map<String, Object> getResultMappings(String resultsFieldName, FieldCapabilitiesResponse fieldCapabilitiesResponse);

    /**
     * @return {@code true} if this analysis supports data frame rows with missing values
     */
    boolean supportsMissingValues();

    /**
     * @return {@code true} if this analysis persists state that can later be used to restore from a given point
     */
    boolean persistsState();

    /**
     * Returns the document id prefix for the analysis state
     */
    String getStateDocIdPrefix(String jobId);

    /**
     * Returns the progress phases the analysis goes through in order
     */
    List<String> getProgressPhases();

    /**
     * @return the analysis inference config or {@code null} if inference is not supported
     */
    @Nullable
    InferenceConfig inferenceConfig(FieldInfo fieldInfo);

    /**
     * @return {@code true} if this analysis trains a model that can be used for inference
     */
    boolean supportsInference();

    /**
     * @return the percentage of data to use for training
     */
    default double getTrainingPercent() {
        return 100.0;
    }
    /**
     * Summarizes information about the fields that is necessary for analysis to generate
     * the parameters needed for the process configuration.
     */
    interface FieldInfo {

        /**
         * Returns the types for the given field or {@code null} if the field is unknown
         * @param field the field whose types to return
         * @return the types for the given field or {@code null} if the field is unknown
         */
        @Nullable
        Set<String> getTypes(String field);

        /**
         * Returns the cardinality of the given field or {@code null} if there is no cardinality for that field
         * @param field the field whose cardinality to get
         * @return the cardinality of the given field or {@code null} if there is no cardinality for that field
         */
        @Nullable
        Long getCardinality(String field);
    }
}
