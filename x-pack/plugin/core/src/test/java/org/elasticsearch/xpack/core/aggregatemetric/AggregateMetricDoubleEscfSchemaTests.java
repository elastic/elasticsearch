/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.aggregatemetric;

import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class AggregateMetricDoubleEscfSchemaTests extends ESTestCase {

    public void testFields() {
        SourceSchema schema = new SourceSchema();
        int metrics = schema.appendNonLeaf("metrics", 0);
        AggregateMetricDoubleEscfSchema aggregate = schema.getSubSchema(metrics, "latency", AggregateMetricDoubleEscfSchema.FACTORY);
        assertThat(schema.leafCount(), equalTo(4));
        assertThat(schema.getFullPath(aggregate.minColIdx()), equalTo("metrics.latency.min"));
        assertThat(schema.getFullPath(aggregate.maxColIdx()), equalTo("metrics.latency.max"));
        assertThat(schema.getFullPath(aggregate.sumColIdx()), equalTo("metrics.latency.sum"));
        assertThat(schema.getFullPath(aggregate.valueCountColIdx()), equalTo("metrics.latency.value_count"));
    }
}
