/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.sourcebatch.SourceSchema;

/**
 * The columns of a {@code histogram} field value, <code>{"values": [...], "counts": [...]}</code>, within a
 * {@link SourceSchema}. Writers that build source batches directly, rather than through XContent, use it to share one
 * definition of the layout the histogram mapper parses.
 *
 * @param countsColIdx the leaf holding the array of counts
 * @param valuesColIdx the leaf holding the array of values, one per count
 */
public record HistogramEscfSchema(int countsColIdx, int valuesColIdx) {

    public static final String COUNTS_FIELD = "counts";
    public static final String VALUES_FIELD = "values";

    /** Registers the fields of a histogram value, see {@link SourceSchema#getSubSchema}. */
    public static final SourceSchema.SubSchemaFactory<HistogramEscfSchema> FACTORY = (schema, parentIdx, name) -> {
        int histogram = schema.appendNonLeaf(name, parentIdx);
        int counts = schema.appendLeaf(COUNTS_FIELD, histogram);
        int values = schema.appendLeaf(VALUES_FIELD, histogram);
        return new HistogramEscfSchema(counts, values);
    };
}
