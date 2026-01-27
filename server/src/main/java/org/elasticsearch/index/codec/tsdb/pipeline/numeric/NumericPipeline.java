/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.BitPackCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.PatchedPForCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ZigzagCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.ZstdCodecStage;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class NumericPipeline implements Closeable {

    private final PipelineDescriptor descriptor;
    private final NumericCodecStage[] stages;
    private final PayloadCodecStage payloadStage;
    private final int blockSize;
    private final int payloadPosition;

    private final EncodingContext encodingContext;
    private final DecodingContext decodingContext;

    private NumericPipeline(final NumericCodecStage[] stages, final PayloadCodecStage payloadStage, int blockSize) {
        this.stages = stages;
        this.payloadStage = payloadStage;
        this.blockSize = blockSize;
        this.payloadPosition = stages.length;

        int totalStages = stages.length + 1;
        byte[] stageIds = new byte[totalStages];
        for (int i = 0; i < stages.length; i++) {
            stageIds[i] = stages[i].id();
        }
        stageIds[stages.length] = payloadStage.id();
        this.descriptor = new PipelineDescriptor(stageIds, blockSize);

        this.encodingContext = new EncodingContext(blockSize, descriptor.pipelineLength());
        this.decodingContext = new DecodingContext(blockSize, stageIds);
    }

    public int blockSize() {
        return blockSize;
    }

    public PipelineDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public void close() throws IOException {
        payloadStage.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stages.length; i++) {
            if (i > 0) {
                sb.append("->");
            }
            sb.append(stages[i].name());
        }
        if (stages.length > 0) {
            sb.append("->");
        }
        sb.append(payloadStage.name());
        return sb.toString();
    }

    public static StageBuilder withBlockSize(int blockSize) {
        return new Builder(blockSize);
    }

    public int encode(final long[] values, int valueCount, final DataOutput out) throws IOException {
        encodingContext.clear();
        return encode(values, valueCount, out, encodingContext);
    }

    public int encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        context.setValueCount(valueCount);

        for (int pos = 0; pos < stages.length; pos++) {
            context.setCurrentPosition(pos);
            int newCount = stages[pos].encode(values, context.valueCount(), context);
            context.setValueCount(newCount);
        }

        context.setCurrentPosition(payloadPosition);
        context.applyStage(payloadPosition);

        BlockFormat.writeBlock(out, values, payloadStage, context);

        return context.valueCount();
    }

    public int decode(final long[] values, final DataInput in) throws IOException {
        decodingContext.clear();
        return decode(values, in, decodingContext);
    }

    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        context.setDataInput(in);

        int valueCount = BlockFormat.readBlock(in, values, payloadStage, context, payloadPosition);

        for (int pos = stages.length - 1; pos >= 0; pos--) {
            if (context.isStageApplied(pos)) {
                valueCount = stages[pos].decode(values, valueCount, context);
            }
        }
        return valueCount;
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
        NumericPipeline build();
    }

    private static final class Builder implements StageBuilder, BuildStage {
        private final List<NumericCodecStage> stages = new ArrayList<>();
        private final int blockSize;
        private PayloadCodecStage payloadStage;

        private Builder(int blockSize) {
            this.blockSize = blockSize;
        }

        @Override
        public StageBuilder delta() {
            stages.add(DeltaCodecStage.INSTANCE);
            return this;
        }

        @Override
        public StageBuilder delta(int minDirectionalChanges) {
            stages.add(new DeltaCodecStage(minDirectionalChanges));
            return this;
        }

        @Override
        public StageBuilder offset() {
            stages.add(OffsetCodecStage.INSTANCE);
            return this;
        }

        @Override
        public StageBuilder offset(int minOffsetRatioPercent) {
            stages.add(new OffsetCodecStage(minOffsetRatioPercent));
            return this;
        }

        @Override
        public StageBuilder gcd() {
            stages.add(GcdCodecStage.INSTANCE);
            return this;
        }

        @Override
        public StageBuilder patchedPFor() {
            stages.add(PatchedPForCodecStage.INSTANCE);
            return this;
        }

        @Override
        public StageBuilder patchedPFor(int maxExceptionPercent) {
            stages.add(new PatchedPForCodecStage(maxExceptionPercent));
            return this;
        }

        @Override
        public StageBuilder zigzag() {
            stages.add(ZigzagCodecStage.INSTANCE);
            return this;
        }

        @Override
        public BuildStage bitPack() {
            this.payloadStage = new BitPackCodecStage(blockSize);
            return this;
        }

        @Override
        public BuildStage zstd() {
            this.payloadStage = new ZstdCodecStage(blockSize, ZstdCodecStage.DEFAULT_COMPRESSION_LEVEL);
            return this;
        }

        @Override
        public BuildStage zstd(int compressionLevel) {
            this.payloadStage = new ZstdCodecStage(blockSize, compressionLevel);
            return this;
        }

        @Override
        public NumericPipeline build() {
            return new NumericPipeline(stages.toArray(NumericCodecStage[]::new), payloadStage, blockSize);
        }
    }
}
