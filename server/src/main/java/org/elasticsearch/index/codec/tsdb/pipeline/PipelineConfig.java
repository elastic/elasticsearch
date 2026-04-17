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
import java.util.stream.Stream;

/**
 * Immutable specification for a field's encoding pipeline.
 *
 * <p>Captures the data type, block size, ordered transform stages, and terminal
 * payload stage. The builder separates transforms from the payload at
 * construction time, making illegal states (e.g. two payloads, payload in the
 * middle) unrepresentable.
 *
 * <p>Use {@link #forLongs} to start building a configuration via the fluent
 * builder API.
 *
 * @param dataType   the numeric data type
 * @param blockSize  the number of values per block (must be a positive power of 2)
 * @param transforms the ordered transform stage specifications
 * @param payload    the terminal payload stage specification
 */
public record PipelineConfig(
    PipelineDescriptor.DataType dataType,
    int blockSize,
    List<StageSpec.TransformSpec> transforms,
    StageSpec.PayloadSpec payload
) {

    /** Validates invariants and creates a defensive copy of the transforms list. */
    public PipelineConfig {
        Objects.requireNonNull(dataType, "dataType must not be null");
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("blockSize must be a positive power of 2, got: " + blockSize);
        }
        Objects.requireNonNull(transforms, "transforms must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        transforms = List.copyOf(transforms);
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
     * Returns all stage specifications in order (transforms followed by payload).
     *
     * @return unmodifiable list of all specs
     */
    public List<StageSpec> specs() {
        return Stream.concat(transforms.stream().map(s -> (StageSpec) s), Stream.of(payload)).toList();
    }

    /**
     * Returns a human-readable description of the pipeline stages.
     *
     * @return the stage names joined by {@code >} delimiters
     */
    public String describeStages() {
        final List<StageSpec> allSpecs = specs();
        if (allSpecs.isEmpty()) {
            return "empty";
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < allSpecs.size(); i++) {
            if (i > 0) {
                sb.append('>');
            }
            sb.append(allSpecs.get(i).stageId().displayName);
        }
        return sb.toString();
    }

    /**
     * Builder for long (integral) pipelines.
     * Provides transform stages and a terminal payload stage.
     */
    public static final class LongBuilder {
        private final int blockSize;
        private final List<StageSpec.TransformSpec> transforms = new ArrayList<>();

        private LongBuilder(int blockSize) {
            this.blockSize = blockSize;
        }

        /**
         * Adds a delta encoding stage.
         *
         * @return this builder
         */
        public LongBuilder delta() {
            transforms.add(new StageSpec.DeltaStage());
            return this;
        }

        /**
         * Adds an offset removal stage.
         *
         * @return this builder
         */
        public LongBuilder offset() {
            transforms.add(new StageSpec.OffsetStage());
            return this;
        }

        /**
         * Adds a GCD factoring stage.
         *
         * @return this builder
         */
        public LongBuilder gcd() {
            transforms.add(new StageSpec.GcdStage());
            return this;
        }

        /**
         * Adds a bit-packing payload and builds the configuration.
         *
         * @return the pipeline configuration
         */
        public PipelineConfig bitPack() {
            return new PipelineConfig(PipelineDescriptor.DataType.LONG, blockSize, transforms, new StageSpec.BitPackPayload());
        }
    }
}
