/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class HistogramEscfSchemaTests extends ESTestCase {

    public void testFields() {
        SourceSchema schema = new SourceSchema();
        int metrics = schema.appendNonLeaf("metrics", 0);
        HistogramEscfSchema histogram = schema.getSubSchema(metrics, "latency", HistogramEscfSchema.FACTORY);
        assertThat(schema.leafCount(), equalTo(2));
        assertThat(schema.getFullPath(histogram.countsColIdx()), equalTo("metrics.latency.counts"));
        assertThat(schema.getFullPath(histogram.valuesColIdx()), equalTo("metrics.latency.values"));
    }
}
