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

    /** Smallest allowed block size; also the default. Must be a power of 2. */
    public static final int MIN_BLOCK_SIZE = 128;

    static final String DATA_CODEC = "ColumNARNumericData";
    static final String DATA_EXTENSION = "cnvd";
    static final String META_CODEC = "ColumNARNumericMeta";
    static final String META_EXTENSION = "cnvm";

    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;

    /**
     * Constructs the format with a custom per-field pipeline selector and an explicit block size.
     * The block size controls how many values are grouped into each encoded block; it must be a
     * power of 2 and at least {@value #MIN_BLOCK_SIZE}.
     *
     * @throws IllegalArgumentException if {@code blockSize} is not a power of 2 >= {@value #MIN_BLOCK_SIZE}
     */
    public ColumNARDocValuesFormat(NumericPipelineSelector pipelineSelector, int blockSize) {
        super(ColumnarFormat.NAME);
        if (blockSize < MIN_BLOCK_SIZE || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("blockSize must be a power of 2 >= " + MIN_BLOCK_SIZE + ", got: " + blockSize);
        }
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
    }

    /** Constructs the format with a custom per-field pipeline selector and the default block size. */
    public ColumNARDocValuesFormat(NumericPipelineSelector pipelineSelector) {
        this(pipelineSelector, MIN_BLOCK_SIZE);
    }

    /** SPI constructor. Uses the default pipeline for every field. */
    public ColumNARDocValuesFormat() {
        this((fieldName, type) -> NumericPipeline::defaultPipeline);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(state, pipelineSelector, blockSize);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }
}
