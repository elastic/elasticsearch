/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Immutable specification for a field's encoding pipeline.
 *
 * <p>Captures the data type, block size, and ordered stage specifications for
 * deferred codec construction. Use {@link #forLongs}, {@link #forDoubles}, or
 * {@link #forFloats} to start building a configuration via the fluent builder API.
 */
public record PipelineConfig(PipelineDescriptor.DataType dataType, int blockSize, List<StageSpec> specs) {

    public PipelineConfig {
        Objects.requireNonNull(dataType, "dataType must not be null");
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("blockSize must be a positive power of 2, got: " + blockSize);
        }
        Objects.requireNonNull(specs, "specs must not be null");
        specs = List.copyOf(specs);
    }

    /**
     * Starts building a long (integral) pipeline configuration.
     *
     * @param blockSize the number of values per block
     * @return a new builder for long pipelines
     */
    public static LongBuilder forLongs(final int blockSize) {
        return new LongBuilder(blockSize);
    }

    /**
     * Starts building a double (floating-point) pipeline configuration.
     *
     * @param blockSize the number of values per block
     * @return a new builder for double pipelines
     */
    public static DoubleBuilder forDoubles(final int blockSize) {
        return new DoubleBuilder(blockSize);
    }

    /**
     * Starts building a float (single-precision floating-point) pipeline configuration.
     *
     * @param blockSize the number of values per block
     * @return a new builder for float pipelines
     */
    public static FloatBuilder forFloats(final int blockSize) {
        return new FloatBuilder(blockSize);
    }

    /**
     * Creates a pipeline configuration directly from components.
     *
     * @param dataType  the numeric data type
     * @param blockSize the number of values per block
     * @param specs     the ordered stage specifications
     * @return the pipeline configuration
     */
    public static PipelineConfig of(final PipelineDescriptor.DataType dataType, final int blockSize, final List<StageSpec> specs) {
        return new PipelineConfig(dataType, blockSize, specs);
    }

    /**
     * Returns a human-readable description of the pipeline stages.
     *
     * @return the stage names joined by {@code >} delimiters
     */
    public String describeStages() {
        if (specs.isEmpty()) {
            return "default";
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < specs.size(); i++) {
            if (i > 0) {
                sb.append('>');
            }
            sb.append(specs.get(i).stageId().displayName);
        }
        return sb.toString();
    }

    /**
     * Builder for long (integral) pipelines.
     * Provides transform stages and terminal payload stages.
     */
    public static final class LongBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private LongBuilder(final int blockSize) {
            this.blockSize = blockSize;
        }

        public LongBuilder delta() {
            specs.add(new StageSpec.DeltaStage());
            return this;
        }

        public LongBuilder offset() {
            specs.add(new StageSpec.OffsetStage());
            return this;
        }

        public LongBuilder gcd() {
            specs.add(new StageSpec.GcdStage());
            return this;
        }

        public LongBuilder patchedPFor() {
            specs.add(new StageSpec.PatchedPForStage());
            return this;
        }

        public LongBuilder xor() {
            specs.add(new StageSpec.XorStage());
            return this;
        }

        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPackPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.LONG, blockSize, specs);
        }

        public PipelineConfig zstd() {
            specs.add(new StageSpec.ZstdPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.LONG, blockSize, specs);
        }

        public PipelineConfig lz4() {
            specs.add(new StageSpec.Lz4Payload());
            return new PipelineConfig(PipelineDescriptor.DataType.LONG, blockSize, specs);
        }

        public PipelineConfig lz4HighCompression() {
            specs.add(new StageSpec.Lz4Payload(true));
            return new PipelineConfig(PipelineDescriptor.DataType.LONG, blockSize, specs);
        }
    }

    /**
     * Builder for double (floating-point) pipelines.
     * Includes all transform stages from {@link LongBuilder} plus double-specific stages.
     */
    public static final class DoubleBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private DoubleBuilder(final int blockSize) {
            this.blockSize = blockSize;
        }

        public DoubleBuilder delta() {
            specs.add(new StageSpec.DeltaStage());
            return this;
        }

        public DoubleBuilder offset() {
            specs.add(new StageSpec.OffsetStage());
            return this;
        }

        public DoubleBuilder gcd() {
            specs.add(new StageSpec.GcdStage());
            return this;
        }

        public DoubleBuilder patchedPFor() {
            specs.add(new StageSpec.PatchedPForStage());
            return this;
        }

        public DoubleBuilder xor() {
            specs.add(new StageSpec.XorStage());
            return this;
        }

        public DoubleBuilder alpDoubleStage() {
            specs.add(new StageSpec.AlpDoubleStage());
            return this;
        }

        public DoubleBuilder alpDoubleStage(final double maxError) {
            specs.add(new StageSpec.AlpDoubleStage(maxError));
            return this;
        }

        public DoubleBuilder fpcStage() {
            specs.add(new StageSpec.FpcDoubleStage());
            return this;
        }

        public DoubleBuilder fpcStage(final int tableSize) {
            specs.add(new StageSpec.FpcDoubleStage(tableSize));
            return this;
        }

        public DoubleBuilder fpcStage(final double maxError) {
            specs.add(new StageSpec.FpcDoubleStage(0, maxError));
            return this;
        }

        public DoubleBuilder fpcStage(final int tableSize, final double maxError) {
            specs.add(new StageSpec.FpcDoubleStage(tableSize, maxError));
            return this;
        }

        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPackPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig zstd() {
            specs.add(new StageSpec.ZstdPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig lz4() {
            specs.add(new StageSpec.Lz4Payload());
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig lz4HighCompression() {
            specs.add(new StageSpec.Lz4Payload(true));
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig gorilla() {
            specs.add(new StageSpec.GorillaDoublePayload());
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig gorilla(final double maxError) {
            specs.add(new StageSpec.GorillaDoublePayload(maxError));
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig chimp() {
            specs.add(new StageSpec.ChimpDoublePayload());
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig chimp(final double maxError) {
            specs.add(new StageSpec.ChimpDoublePayload(maxError));
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig chimp128() {
            specs.add(new StageSpec.Chimp128DoublePayload());
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig chimp128(final double maxError) {
            specs.add(new StageSpec.Chimp128DoublePayload(maxError));
            return new PipelineConfig(PipelineDescriptor.DataType.DOUBLE, blockSize, specs);
        }
    }

    /**
     * Builder for float (single-precision floating-point) pipelines.
     */
    public static final class FloatBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private FloatBuilder(final int blockSize) {
            this.blockSize = blockSize;
        }

        public FloatBuilder delta() {
            specs.add(new StageSpec.DeltaStage());
            return this;
        }

        public FloatBuilder offset() {
            specs.add(new StageSpec.OffsetStage());
            return this;
        }

        public FloatBuilder gcd() {
            specs.add(new StageSpec.GcdStage());
            return this;
        }

        public FloatBuilder patchedPFor() {
            specs.add(new StageSpec.PatchedPForStage());
            return this;
        }

        public FloatBuilder xor() {
            specs.add(new StageSpec.XorStage());
            return this;
        }

        public FloatBuilder alpFloatStage() {
            specs.add(new StageSpec.AlpFloatStage());
            return this;
        }

        public FloatBuilder alpFloatStage(final double maxError) {
            specs.add(new StageSpec.AlpFloatStage(maxError));
            return this;
        }

        public FloatBuilder fpcStage() {
            specs.add(new StageSpec.FpcFloatStage());
            return this;
        }

        public FloatBuilder fpcStage(final int tableSize) {
            specs.add(new StageSpec.FpcFloatStage(tableSize));
            return this;
        }

        public FloatBuilder fpcStage(final double maxError) {
            specs.add(new StageSpec.FpcFloatStage(0, maxError));
            return this;
        }

        public FloatBuilder fpcStage(final int tableSize, final double maxError) {
            specs.add(new StageSpec.FpcFloatStage(tableSize, maxError));
            return this;
        }

        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPackPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig zstd() {
            specs.add(new StageSpec.ZstdPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig lz4() {
            specs.add(new StageSpec.Lz4Payload());
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig lz4HighCompression() {
            specs.add(new StageSpec.Lz4Payload(true));
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig gorilla() {
            specs.add(new StageSpec.GorillaFloatPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig chimp() {
            specs.add(new StageSpec.ChimpFloatPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig chimp128() {
            specs.add(new StageSpec.Chimp128FloatPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.FLOAT, blockSize, specs);
        }
    }
}
