/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.util.List;
import java.util.Map;

public interface TrainedModel extends NamedXContentObject, NamedWriteable {

    /**
     * @return List of featureNames expected by the model. In the order that they are expected
     */
    List<String> getFeatureNames();

    /**
     * Infer against the provided fields
     *
     * @param fields The fields and their values to infer against
     * @return The predicted value. For classification this will be discrete values (e.g. 0.0, or 1.0).
     *                              For regression this is continuous.
     */
    double infer(Map<String, Object> fields);

    /**
     * @param fields similar to {@link TrainedModel#infer(Map)}, but fields are already in order and doubles
     * @return The predicted value.
     */
    double infer(List<Double> fields);

    /**
     * @return {@link TargetType} for the model.
     */
    TargetType targetType();

    /**
     * This gathers the probabilities for each potential classification value.
     *
     * The probabilities are indexed by classification ordinal label encoding.
     * The length of this list is equal to the number of classification labels.
     *
     * This only should return if the implementation model is inferring classification values and not regression
     * @param fields The fields and their values to infer against
     * @return The probabilities of each classification value
     */
    List<Double> classificationProbability(Map<String, Object> fields);

    /**
     * @param fields similar to {@link TrainedModel#classificationProbability(Map)} but the fields are already in order and doubles
     * @return The probabilities of each classification value
     */
    List<Double> classificationProbability(List<Double> fields);

    /**
     * The ordinal encoded list of the classification labels.
     * @return Oridinal encoded list of classification labels.
     */
    @Nullable
    List<String> classificationLabels();

    /**
     * Runs validations against the model.
     *
     * Example: {@link org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree} should check if there are any loops
     *
     * @throws org.elasticsearch.ElasticsearchException if validations fail
     */
    void validate();
}
