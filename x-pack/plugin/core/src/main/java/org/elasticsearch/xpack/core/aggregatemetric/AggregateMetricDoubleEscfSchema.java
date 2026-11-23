/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.aggregatemetric;

import org.elasticsearch.sourcebatch.SourceSchema;

/**
 * The columns of an {@code aggregate_metric_double} field value, <code>{"min": ..., "max": ..., "sum": ...,
 * "value_count": ...}</code>, within a {@link SourceSchema}. Writers that build source batches directly, rather than
 * through XContent, use it to share one definition of the layout the {@code aggregate_metric_double} mapper parses.
 *
 * <p>All four metrics are registered, whichever of them the field is mapped with. A value only writes the metrics its
 * field has; the others stay absent.
 */
public record AggregateMetricDoubleEscfSchema(int minColIdx, int maxColIdx, int sumColIdx, int valueCountColIdx) {

    public static final String MIN_FIELD = "min";
    public static final String MAX_FIELD = "max";
    public static final String SUM_FIELD = "sum";
    public static final String VALUE_COUNT_FIELD = "value_count";

    /** Registers the fields of an {@code aggregate_metric_double} value, see {@link SourceSchema#getSubSchema}. */
    public static final SourceSchema.SubSchemaFactory<AggregateMetricDoubleEscfSchema> FACTORY = (schema, parentIdx, name) -> {
        int aggregate = schema.appendNonLeaf(name, parentIdx);
        int min = schema.appendLeaf(MIN_FIELD, aggregate);
        int max = schema.appendLeaf(MAX_FIELD, aggregate);
        int sum = schema.appendLeaf(SUM_FIELD, aggregate);
        int valueCount = schema.appendLeaf(VALUE_COUNT_FIELD, aggregate);
        return new AggregateMetricDoubleEscfSchema(min, max, sum, valueCount);
    };
}
