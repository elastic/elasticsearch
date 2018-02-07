/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.List;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.xpack.sql.planner.PlanningException;

public class MatrixStatsAgg extends LeafAgg {

    private final List<String> fields;

    public MatrixStatsAgg(String id, String propertyPath, List<String> fields) {
        super(id, propertyPath, "<multi-field>");
        this.fields = fields;
        throw new PlanningException("innerkey/matrix stats not handled (yet)", RestStatus.BAD_REQUEST);
    }

    @Override
    AggregationBuilder toBuilder() {
        throw new UnsupportedOperationException();
    }
}
