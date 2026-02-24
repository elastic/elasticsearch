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
 * Captures the data type, block size, and stage specifications for deferred codec construction.
 * Use {@link #forLongs} or {@link #forDoubles} to start building a configuration.
 */
public final class PipelineConfig {

    public enum DataType {
        LONG,
        DOUBLE,
        FLOAT
    }

    private final DataType dataType;
    private final int blockSize;
    private final List<StageSpec> specs;

    PipelineConfig(DataType dataType, int blockSize, List<StageSpec> specs) {
        this.dataType = dataType;
        this.blockSize = blockSize;
        this.specs = List.copyOf(specs);
    }

    /**
     * Starts building a long (integral) pipeline configuration.
     */
    public static LongBuilder forLongs(int blockSize) {
        return new LongBuilder(blockSize);
    }

    /**
     * Starts building a double (floating-point) pipeline configuration.
     */
    public static DoubleBuilder forDoubles(int blockSize) {
        return new DoubleBuilder(blockSize);
    }

    public static FloatBuilder forFloats(int blockSize) {
        return new FloatBuilder(blockSize);
    }

    public static PipelineConfig of(final DataType dataType, int blockSize, final List<StageSpec> specs) {
        return new PipelineConfig(dataType, blockSize, specs);
    }

    public DataType dataType() {
        return dataType;
    }

    public int blockSize() {
        return blockSize;
    }

    /**
     * Returns an unmodifiable view of the stage specifications.
     */
    public List<StageSpec> specs() {
        return specs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof PipelineConfig that) {
            return dataType == that.dataType && blockSize == that.blockSize && specs.equals(that.specs);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, blockSize, specs);
    }

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

    @Override
    public String toString() {
        return "PipelineConfig{" + "dataType=" + dataType + ", blockSize=" + blockSize + ", specs=" + specs + '}';
    }

    /**
     * Builder for long (integral) pipelines.
     * Provides transform stages and terminal payload stages.
     */
    public static final class LongBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private LongBuilder(int blockSize) {
            this.blockSize = blockSize;
        }

        public LongBuilder delta() {
            specs.add(new StageSpec.Delta());
            return this;
        }

        public LongBuilder deltaDelta() {
            specs.add(new StageSpec.DeltaDelta());
            return this;
        }

        public LongBuilder offset() {
            specs.add(new StageSpec.Offset());
            return this;
        }

        public LongBuilder gcd() {
            specs.add(new StageSpec.Gcd());
            return this;
        }

        public LongBuilder rle() {
            specs.add(new StageSpec.Rle());
            return this;
        }

        public LongBuilder patchedPFor() {
            specs.add(new StageSpec.PatchedPFor());
            return this;
        }

        public LongBuilder xor() {
            specs.add(new StageSpec.Xor());
            return this;
        }

        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPack());
            return new PipelineConfig(DataType.LONG, blockSize, specs);
        }

        public PipelineConfig zstd() {
            specs.add(new StageSpec.Zstd());
            return new PipelineConfig(DataType.LONG, blockSize, specs);
        }

        public PipelineConfig lz4() {
            specs.add(new StageSpec.Lz4());
            return new PipelineConfig(DataType.LONG, blockSize, specs);
        }

        public PipelineConfig lz4HighCompression() {
            specs.add(new StageSpec.Lz4(true));
            return new PipelineConfig(DataType.LONG, blockSize, specs);
        }

        public PipelineConfig rlePayload() {
            specs.add(new StageSpec.RlePayload());
            return new PipelineConfig(DataType.LONG, blockSize, specs);
        }
    }

    /**
     * Builder for double (floating-point) pipelines.
     * Includes all transform stages from LongBuilder plus double-specific stages.
     */
    public static final class DoubleBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private DoubleBuilder(int blockSize) {
            this.blockSize = blockSize;
        }

        public DoubleBuilder delta() {
            specs.add(new StageSpec.Delta());
            return this;
        }

        public DoubleBuilder deltaDelta() {
            specs.add(new StageSpec.DeltaDelta());
            return this;
        }

        public DoubleBuilder offset() {
            specs.add(new StageSpec.Offset());
            return this;
        }

        public DoubleBuilder gcd() {
            specs.add(new StageSpec.Gcd());
            return this;
        }

        public DoubleBuilder rle() {
            specs.add(new StageSpec.Rle());
            return this;
        }

        public DoubleBuilder patchedPFor() {
            specs.add(new StageSpec.PatchedPFor());
            return this;
        }

        public DoubleBuilder xor() {
            specs.add(new StageSpec.Xor());
            return this;
        }

        public DoubleBuilder quantizeDouble(double maxError) {
            specs.add(new StageSpec.QuantizeDouble(maxError));
            return this;
        }

        public DoubleBuilder alpDoubleStage() {
            specs.add(new StageSpec.AlpDoubleStage());
            return this;
        }

        public DoubleBuilder alpDoubleStage(double maxError) {
            specs.add(new StageSpec.AlpDoubleStage(maxError));
            return this;
        }

        public DoubleBuilder alpRdDoubleStage() {
            specs.add(new StageSpec.AlpRdDoubleStage());
            return this;
        }

        public DoubleBuilder alpRdDoubleStage(double maxError) {
            specs.add(new StageSpec.AlpRdDoubleStage(maxError));
            return this;
        }

        public DoubleBuilder fpcStage() {
            specs.add(new StageSpec.FpcStage());
            return this;
        }

        public DoubleBuilder fpcStage(int tableSize) {
            specs.add(new StageSpec.FpcStage(tableSize));
            return this;
        }

        public DoubleBuilder fpcStage(double maxError) {
            specs.add(new StageSpec.FpcStage(0, maxError));
            return this;
        }

        public DoubleBuilder fpcStage(int tableSize, double maxError) {
            specs.add(new StageSpec.FpcStage(tableSize, maxError));
            return this;
        }

        public DoubleBuilder chimpDoubleStage() {
            specs.add(new StageSpec.ChimpDoubleStage());
            return this;
        }

        public DoubleBuilder chimpDoubleStage(int groupSize) {
            specs.add(new StageSpec.ChimpDoubleStage(groupSize));
            return this;
        }

        public DoubleBuilder chimpDoubleStage(double maxError) {
            specs.add(new StageSpec.ChimpDoubleStage(16, maxError));
            return this;
        }

        public DoubleBuilder chimpDoubleStage(int groupSize, double maxError) {
            specs.add(new StageSpec.ChimpDoubleStage(groupSize, maxError));
            return this;
        }

        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPack());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig zstd() {
            specs.add(new StageSpec.Zstd());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig lz4() {
            specs.add(new StageSpec.Lz4());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig lz4HighCompression() {
            specs.add(new StageSpec.Lz4(true));
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig rlePayload() {
            specs.add(new StageSpec.RlePayload());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig gorilla() {
            specs.add(new StageSpec.Gorilla());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig alpDouble() {
            specs.add(new StageSpec.AlpDouble());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig alpDouble(double maxError) {
            specs.add(new StageSpec.AlpDouble(maxError));
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig alpRdDouble() {
            specs.add(new StageSpec.AlpRdDouble());
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }

        public PipelineConfig alpRdDouble(double maxError) {
            specs.add(new StageSpec.AlpRdDouble(maxError));
            return new PipelineConfig(DataType.DOUBLE, blockSize, specs);
        }
    }

    public static final class FloatBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private FloatBuilder(int blockSize) {
            this.blockSize = blockSize;
        }

        public FloatBuilder delta() {
            specs.add(new StageSpec.Delta());
            return this;
        }

        public FloatBuilder deltaDelta() {
            specs.add(new StageSpec.DeltaDelta());
            return this;
        }

        public FloatBuilder offset() {
            specs.add(new StageSpec.Offset());
            return this;
        }

        public FloatBuilder gcd() {
            specs.add(new StageSpec.Gcd());
            return this;
        }

        public FloatBuilder rle() {
            specs.add(new StageSpec.Rle());
            return this;
        }

        public FloatBuilder patchedPFor() {
            specs.add(new StageSpec.PatchedPFor());
            return this;
        }

        public FloatBuilder xor() {
            specs.add(new StageSpec.Xor());
            return this;
        }

        public FloatBuilder alpFloatStage() {
            specs.add(new StageSpec.AlpFloatStage());
            return this;
        }

        public FloatBuilder alpFloatStage(double maxError) {
            specs.add(new StageSpec.AlpFloatStage(maxError));
            return this;
        }

        public FloatBuilder fpcStage() {
            specs.add(new StageSpec.FpcStage());
            return this;
        }

        public FloatBuilder fpcStage(int tableSize) {
            specs.add(new StageSpec.FpcStage(tableSize));
            return this;
        }

        public FloatBuilder fpcStage(double maxError) {
            specs.add(new StageSpec.FpcStage(0, maxError));
            return this;
        }

        public FloatBuilder fpcStage(int tableSize, double maxError) {
            specs.add(new StageSpec.FpcStage(tableSize, maxError));
            return this;
        }

        public FloatBuilder alpRdFloatStage() {
            specs.add(new StageSpec.AlpRdFloatStage());
            return this;
        }

        public FloatBuilder alpRdFloatStage(double maxError) {
            specs.add(new StageSpec.AlpRdFloatStage(maxError));
            return this;
        }

        public FloatBuilder chimpFloatStage() {
            specs.add(new StageSpec.ChimpFloatStage());
            return this;
        }

        public FloatBuilder chimpFloatStage(int groupSize) {
            specs.add(new StageSpec.ChimpFloatStage(groupSize));
            return this;
        }

        public FloatBuilder chimpFloatStage(double maxError) {
            specs.add(new StageSpec.ChimpFloatStage(16, maxError));
            return this;
        }

        public FloatBuilder chimpFloatStage(int groupSize, double maxError) {
            specs.add(new StageSpec.ChimpFloatStage(groupSize, maxError));
            return this;
        }

        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPack());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig zstd() {
            specs.add(new StageSpec.Zstd());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig lz4() {
            specs.add(new StageSpec.Lz4());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig lz4HighCompression() {
            specs.add(new StageSpec.Lz4(true));
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig rlePayload() {
            specs.add(new StageSpec.RlePayload());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig gorilla() {
            specs.add(new StageSpec.GorillaFloat());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig alpFloat() {
            specs.add(new StageSpec.AlpFloat());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig alpFloat(double maxError) {
            specs.add(new StageSpec.AlpFloat(maxError));
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig alpRdFloat() {
            specs.add(new StageSpec.AlpRdFloat());
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }

        public PipelineConfig alpRdFloat(double maxError) {
            specs.add(new StageSpec.AlpRdFloat(maxError));
            return new PipelineConfig(DataType.FLOAT, blockSize, specs);
        }
    }
}
