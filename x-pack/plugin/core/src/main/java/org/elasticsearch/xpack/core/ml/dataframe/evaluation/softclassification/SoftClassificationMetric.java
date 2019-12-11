/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;

public interface SoftClassificationMetric extends EvaluationMetric {

    static QueryBuilder actualIsTrueQuery(String actualField) {
        return QueryBuilders.queryStringQuery(actualField + ": (1 OR true)");
    }
}
