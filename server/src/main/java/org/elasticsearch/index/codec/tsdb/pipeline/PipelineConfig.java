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
 * deferred codec construction. Use {@link #forLongs} to start building a
 * configuration via the fluent builder API.
 *
 * @param dataType  the numeric data type
 * @param blockSize the number of values per block (must be a positive power of 2)
 * @param specs     the ordered stage specifications
 */
public record PipelineConfig(PipelineDescriptor.DataType dataType, int blockSize, List<StageSpec> specs) {

    /** Validates invariants and creates a defensive copy of the specs list. */
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
    public static LongBuilder forLongs(int blockSize) {
        return new LongBuilder(blockSize);
    }

    /**
     * Creates a pipeline configuration directly from components.
     *
     * @param dataType  the numeric data type
     * @param blockSize the number of values per block
     * @param specs     the ordered stage specifications
     * @return the pipeline configuration
     */
    public static PipelineConfig of(final PipelineDescriptor.DataType dataType, int blockSize, final List<StageSpec> specs) {
        return new PipelineConfig(dataType, blockSize, specs);
    }

    /**
     * Returns a human-readable description of the pipeline stages.
     *
     * @return the stage names joined by {@code >} delimiters
     */
    public String describeStages() {
        if (specs.isEmpty()) {
            return "empty";
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
     * Provides transform stages and a terminal payload stage.
     */
    public static final class LongBuilder {
        private final int blockSize;
        private final List<StageSpec> specs = new ArrayList<>();

        private LongBuilder(int blockSize) {
            this.blockSize = blockSize;
        }

        /**
         * Adds a delta encoding stage.
         *
         * @return this builder
         */
        public LongBuilder delta() {
            specs.add(new StageSpec.DeltaStage());
            return this;
        }

        /**
         * Adds an offset removal stage.
         *
         * @return this builder
         */
        public LongBuilder offset() {
            specs.add(new StageSpec.OffsetStage());
            return this;
        }

        /**
         * Adds a GCD factoring stage.
         *
         * @return this builder
         */
        public LongBuilder gcd() {
            specs.add(new StageSpec.GcdStage());
            return this;
        }

        /**
         * Adds a bit-packing payload and builds the configuration.
         *
         * @return the pipeline configuration
         */
        public PipelineConfig bitPack() {
            specs.add(new StageSpec.BitPackPayload());
            return new PipelineConfig(PipelineDescriptor.DataType.LONG, blockSize, specs);
        }
    }
}
