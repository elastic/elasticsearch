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
import java.util.Objects;

/**
 * A binary Lucene {@link DocValuesFormat}: every field is a {@code BinaryDocValues} column tagged with a
 * {@link ColumnarFieldType} ({@link #TYPE_ATTRIBUTE}), served through this library's own range-query and
 * block-loader APIs. The typed doc-values shapes are rejected.
 *
 * <p>Pipeline selection is delegated to the injected {@link NumericPipelineSelector}. Callers that
 * need per-field encoding (e.g. ALP for doubles, SplitDelta for counters) supply a concrete
 * implementation via {@link Builder}. The no-arg SPI constructor uses the default pipeline for
 * every field.
 */
public class ColumNARDocValuesFormat extends DocValuesFormat {

    /** {@link org.apache.lucene.index.FieldInfo} attribute naming a field's {@link ColumnarFieldType}. The mapper sets it. */
    public static final String TYPE_ATTRIBUTE = "columnar.type";

    /** Smallest allowed block size. Must be a power of 2. */
    public static final int MIN_BLOCK_SIZE = 128;

    /** Largest allowed block size. Caps O(blockSize) per-field allocations in the encoder. */
    public static final int MAX_BLOCK_SIZE = 8192;

    /** Default block size used when none is specified. */
    public static final int DEFAULT_BLOCK_SIZE = MIN_BLOCK_SIZE;

    static final String DATA_CODEC = "ColumNARData";
    static final String DATA_EXTENSION = "cnd";
    static final String META_CODEC = "ColumNARMeta";
    static final String META_EXTENSION = "cnm";

    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;
    private final ColumnarWriteProfile writeProfile;

    /** SPI constructor. Uses the default pipeline for every field. */
    public ColumNARDocValuesFormat() {
        this((fieldName, type) -> (profile, bs) -> NumericPipeline.defaultPipeline(bs), DEFAULT_BLOCK_SIZE, ColumnarWriteProfile.current());
    }

    private ColumNARDocValuesFormat(
        final NumericPipelineSelector pipelineSelector,
        int blockSize,
        final ColumnarWriteProfile writeProfile
    ) {
        super(ColumnarFormat.NAME);
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException(
                "blockSize must be a power of 2 in [" + MIN_BLOCK_SIZE + ", " + MAX_BLOCK_SIZE + "], got: " + blockSize
            );
        }
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
        this.writeProfile = writeProfile;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(state, pipelineSelector, blockSize, writeProfile);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }

    /**
     * Builder for {@link ColumNARDocValuesFormat}. All parameters are optional and default to the
     * same values as the no-arg SPI constructor.
     */
    public static final class Builder {

        private NumericPipelineSelector pipelineSelector = (fieldName, type) -> (profile, bs) -> NumericPipeline.defaultPipeline(bs);
        private int blockSize = DEFAULT_BLOCK_SIZE;
        private ColumnarWriteProfile writeProfile = ColumnarWriteProfile.current();

        public Builder() {}

        /** Sets the per-field pipeline selector. */
        public Builder pipelineSelector(final NumericPipelineSelector pipelineSelector) {
            this.pipelineSelector = Objects.requireNonNull(pipelineSelector);
            return this;
        }

        /**
         * Sets the block size. Must be a power of 2 between {@value ColumNARDocValuesFormat#MIN_BLOCK_SIZE}
         * and {@value ColumNARDocValuesFormat#MAX_BLOCK_SIZE} inclusive.
         */
        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /** Sets the write profile controlling the format version stamped into new segments and which feature ids may be emitted. */
        public Builder writeProfile(final ColumnarWriteProfile writeProfile) {
            this.writeProfile = Objects.requireNonNull(writeProfile);
            return this;
        }

        public ColumNARDocValuesFormat build() {
            return new ColumNARDocValuesFormat(pipelineSelector, blockSize, writeProfile);
        }
    }
}
