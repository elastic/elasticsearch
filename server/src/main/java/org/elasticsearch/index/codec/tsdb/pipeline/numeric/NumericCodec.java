/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

import java.io.Closeable;
import java.io.IOException;

public final class NumericCodec implements Closeable {

    private final NumericPipeline pipeline;

    private NumericCodec(final NumericPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public static StageBuilder withBlockSize(int blockSize) {
        return new Builder(blockSize);
    }

    public NumericBlockEncoder newEncoder() {
        return new NumericBlockEncoder(pipeline);
    }

    public NumericBlockDecoder newDecoder() {
        return new NumericBlockDecoder(pipeline);
    }

    public PipelineDescriptor descriptor() {
        return pipeline.descriptor();
    }

    public int blockSize() {
        return pipeline.blockSize();
    }

    @Override
    public void close() throws IOException {
        pipeline.close();
    }

    @Override
    public String toString() {
        return pipeline.toString();
    }

    public interface StageBuilder {
        StageBuilder delta();

        StageBuilder delta(int minDirectionalChanges);

        StageBuilder offset();

        StageBuilder offset(int minOffsetRatioPercent);

        StageBuilder gcd();

        StageBuilder patchedPFor();

        StageBuilder patchedPFor(int maxExceptionPercent);

        StageBuilder zigzag();

        BuildStage bitPack();

        BuildStage zstd();

        BuildStage zstd(int compressionLevel);
    }

    public interface BuildStage {
        NumericCodec build();
    }

    private static final class Builder implements StageBuilder, BuildStage {
        private NumericPipeline.StageBuilder stageBuilder;
        private NumericPipeline.BuildStage buildStage;

        private Builder(int blockSize) {
            this.stageBuilder = NumericPipeline.withBlockSize(blockSize);
        }

        @Override
        public StageBuilder delta() {
            stageBuilder = stageBuilder.delta();
            return this;
        }

        @Override
        public StageBuilder delta(int minDirectionalChanges) {
            stageBuilder = stageBuilder.delta(minDirectionalChanges);
            return this;
        }

        @Override
        public StageBuilder offset() {
            stageBuilder = stageBuilder.offset();
            return this;
        }

        @Override
        public StageBuilder offset(int minOffsetRatioPercent) {
            stageBuilder = stageBuilder.offset(minOffsetRatioPercent);
            return this;
        }

        @Override
        public StageBuilder gcd() {
            stageBuilder = stageBuilder.gcd();
            return this;
        }

        @Override
        public StageBuilder patchedPFor() {
            stageBuilder = stageBuilder.patchedPFor();
            return this;
        }

        @Override
        public StageBuilder patchedPFor(int maxExceptionPercent) {
            stageBuilder = stageBuilder.patchedPFor(maxExceptionPercent);
            return this;
        }

        @Override
        public StageBuilder zigzag() {
            stageBuilder = stageBuilder.zigzag();
            return this;
        }

        @Override
        public BuildStage bitPack() {
            buildStage = stageBuilder.bitPack();
            return this;
        }

        @Override
        public BuildStage zstd() {
            buildStage = stageBuilder.zstd();
            return this;
        }

        @Override
        public BuildStage zstd(int compressionLevel) {
            buildStage = stageBuilder.zstd(compressionLevel);
            return this;
        }

        @Override
        public NumericCodec build() {
            return new NumericCodec(buildStage.build());
        }
    }
}
