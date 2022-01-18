/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search;

/**
 * Reference to a ES aggregation (which can be either a GROUP BY or Metric agg).
 */
public abstract class AggRef implements FieldExtraction {

    @Override
    public void collectFields(QlSourceBuilder sourceBuilder) {
        // Aggregations do not need any special fields
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return true;
    }
}
