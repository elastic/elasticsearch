/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * The result of an evaluation metric
 */
public interface EvaluationMetricResult extends ToXContentObject, NamedWriteable {

    /**
     * Returns the name of the metric (which may differ to the writeable name)
     */
    String getMetricName();
}
