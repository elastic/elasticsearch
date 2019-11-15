/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Optional;

/**
 * {@link EvaluationMetric} class represents a metric to evaluate.
 */
public interface EvaluationMetric extends ToXContentObject, NamedWriteable {

    /**
     * Returns the name of the metric (which may differ to the writeable name)
     */
    String getName();

    /**
     * Gets the evaluation result for this metric.
     * @return {@code Optional.empty()} if the result is not available yet, {@code Optional.of(result)} otherwise
     */
    Optional<EvaluationMetricResult> getResult();
}
