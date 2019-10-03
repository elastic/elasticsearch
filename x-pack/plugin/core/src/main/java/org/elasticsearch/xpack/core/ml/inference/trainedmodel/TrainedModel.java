/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

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
     * @return {@code true} if the model is classification, {@code false} otherwise.
     */
    boolean isClassification();

    /**
     * This gathers the probabilities for each potential classification value.
     *
     * This only should return if the implementation model is inferring classification values and not regression
     * @param fields The fields and their values to infer against
     * @return The probabilities of each classification value
     */
    List<Double> inferProbabilities(Map<String, Object> fields);

}
