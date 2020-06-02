/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.AggRef;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;

public class PivotColumnRef extends AggRef {

    private final FieldExtraction agg;
    private final FieldExtraction pivot;
    private final Object value;

    public PivotColumnRef(FieldExtraction pivot, FieldExtraction agg, Object value) {
        this.pivot = pivot;
        this.agg = agg;
        // due to the way Elasticsearch aggs work
        // promote the object to expect types so that the comparison works
        this.value = esAggType(value);
    }

    private static Object esAggType(Object value) {
        if (value instanceof Number) {
            Number n = (Number) value;
            if (value instanceof Double) {
                return value;
            }
            if (value instanceof Float) {
                return Double.valueOf(n.doubleValue());
            }
            return Long.valueOf(n.longValue());
        }
        return value;
    }

    public FieldExtraction pivot() {
        return pivot;
    }

    public FieldExtraction agg() {
        return agg;
    }

    public Object value() {
        return value;
    }
}
