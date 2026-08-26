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
 * implementation via the two-arg constructor. The no-arg SPI constructor uses the default pipeline for
 * every field.
 */
public class ColumNARDocValuesFormat extends DocValuesFormat {

    /** {@link org.apache.lucene.index.FieldInfo} attribute naming a field's {@link ColumnarFieldType}. The mapper sets it. */
    public static final String TYPE_ATTRIBUTE = "columnar.type";

    /** Smallest allowed block size. Must be a power of 2. */
    public static final int MIN_BLOCK_SIZE = 128;

    /**
     * Largest allowed block size, in values. This caps the per-field allocations a column makes for one block —
     * exactly, at {@code long[blockSize]}, for a numeric column. A string column's block buffer holds
     * {@code blockSize} values, whose byte size is a property of the data rather than of this cap; bounding
     * those bytes is what the byte-derived chunking in {@code docs/PLAN.md} is for.
     */
    public static final int MAX_BLOCK_SIZE = 8192;

    /** Default block size used when none is specified. */
    public static final int DEFAULT_BLOCK_SIZE = MIN_BLOCK_SIZE;

    static final String DATA_CODEC = "ColumNARData";
    static final String DATA_EXTENSION = "cnd";
    static final String META_CODEC = "ColumNARMeta";
    static final String META_EXTENSION = "cnm";
    static final String SKIP_CODEC = "ColumNARSkipIndex";
    static final String SKIP_EXTENSION = "cns";

    private final NumericPipelineSelector pipelineSelector;
    private final ColumnarFieldTypeSelector typeSelector;
    private final int blockSize;

    /** SPI constructor. Uses the default pipeline for every field and reads each field's type from its attribute. */
    public ColumNARDocValuesFormat() {
        this((fieldName, type) -> NumericPipeline::defaultPipeline, ColumnarFieldType::fromField, DEFAULT_BLOCK_SIZE);
    }

    /** Constructs a format with a custom type selector, using the default pipeline and block size. */
    public ColumNARDocValuesFormat(final ColumnarFieldTypeSelector typeSelector) {
        this((fieldName, type) -> NumericPipeline::defaultPipeline, typeSelector, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Constructs a format with a custom pipeline selector and block size. Field types are read from their attribute.
     * {@code blockSize} must be a power of 2 in [{@value #MIN_BLOCK_SIZE}, {@value #MAX_BLOCK_SIZE}].
     */
    public ColumNARDocValuesFormat(final NumericPipelineSelector pipelineSelector, int blockSize) {
        this(pipelineSelector, ColumnarFieldType::fromField, blockSize);
    }

    /**
     * Constructs a format with a custom pipeline selector, type selector, and block size.
     * {@code blockSize} must be a power of 2 in [{@value #MIN_BLOCK_SIZE}, {@value #MAX_BLOCK_SIZE}].
     */
    public ColumNARDocValuesFormat(
        final NumericPipelineSelector pipelineSelector,
        final ColumnarFieldTypeSelector typeSelector,
        int blockSize
    ) {
        super(ColumnarFormat.NAME);
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException(
                "blockSize must be a power of 2 in [" + MIN_BLOCK_SIZE + ", " + MAX_BLOCK_SIZE + "], got: " + blockSize
            );
        }
        this.pipelineSelector = pipelineSelector;
        this.typeSelector = typeSelector;
        this.blockSize = blockSize;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(state, pipelineSelector, typeSelector, blockSize);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }
}
