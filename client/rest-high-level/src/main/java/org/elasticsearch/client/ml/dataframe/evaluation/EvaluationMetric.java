/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation;

import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Defines an evaluation metric
 */
public interface EvaluationMetric extends ToXContentObject {

    /**
     * Returns the name of the metric
     */
    String getName();

    /**
     * The result of an evaluation metric
     */
    interface Result extends ToXContentObject {

        /**
         * Returns the name of the metric
         */
        String getMetricName();
    }
}
