/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;

import java.io.IOException;

/**
 * A binary Lucene {@link DocValuesFormat}: every field is a {@code BinaryDocValues} column tagged with a
 * {@link ColumnarFieldType} ({@link #TYPE_ATTRIBUTE}), served through this library's own range-query and
 * block-loader APIs. The typed doc-values shapes are rejected.
 *
 * <p>Pipeline selection is delegated to the injected {@link NumericPipelineSelector}. Callers that
 * need per-field encoding (e.g. ALP for doubles, SplitDelta for counters) supply a concrete
 * implementation at construction time. The no-arg SPI constructor uses the default pipeline for
 * every field, preserving backward-compatible behavior.
 */
public class ColumNARDocValuesFormat extends DocValuesFormat {

    /** {@link org.apache.lucene.index.FieldInfo} attribute naming a field's {@link ColumnarFieldType}. The mapper sets it. */
    public static final String TYPE_ATTRIBUTE = "columnar.type";

    static final String DATA_CODEC = "ColumNARNumericData";
    static final String DATA_EXTENSION = "cnvd";
    static final String META_CODEC = "ColumNARNumericMeta";
    static final String META_EXTENSION = "cnvm";

    private final NumericPipelineSelector pipelineSelector;

    /**
     * Constructs the format with a custom per-field pipeline selector. The server module supplies
     * an implementation that inspects field type, index mode, and metric role via the mapper.
     */
    public ColumNARDocValuesFormat(NumericPipelineSelector pipelineSelector) {
        super(ColumnarFormat.NAME);
        this.pipelineSelector = pipelineSelector;
    }

    /**
     * SPI constructor. Uses the default pipeline (delta, offset, GCD, FOR) for every field.
     * Existing callers and tests that do not need per-field selection use this constructor.
     */
    public ColumNARDocValuesFormat() {
        this((fieldName, type, bs) -> NumericPipeline.defaultPipeline(bs));
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(state, pipelineSelector);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }
}
