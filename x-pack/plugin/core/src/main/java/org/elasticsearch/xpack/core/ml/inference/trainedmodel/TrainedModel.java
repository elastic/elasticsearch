/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.util.Map;

public interface TrainedModel extends NamedXContentObject, NamedWriteable, Accountable {

    /**
     * Infer against the provided fields
     *
     * @param fields The fields and their values to infer against
     * @param config The configuration options for inference
     * @return The predicted value. For classification this will be discrete values (e.g. 0.0, or 1.0).
     *                              For regression this is continuous.
     */
    InferenceResults infer(Map<String, Object> fields, InferenceConfig config);

    /**
     * @return {@link TargetType} for the model.
     */
    TargetType targetType();

    /**
     * Runs validations against the model.
     *
     * Example: {@link org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree} should check if there are any loops
     *
     * @throws org.elasticsearch.ElasticsearchException if validations fail
     */
    void validate();

    /**
     * @return The estimated number of operations required at inference time
     */
    long estimatedNumOperations();
}
