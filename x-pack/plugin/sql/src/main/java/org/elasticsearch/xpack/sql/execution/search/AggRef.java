/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

/**
 * Reference to a ES aggregation (which can be either a GROUP BY or Metric agg).
 */
public abstract class AggRef implements FieldExtraction {

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        // Aggregations do not need any special fields
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return true;
    }
}
