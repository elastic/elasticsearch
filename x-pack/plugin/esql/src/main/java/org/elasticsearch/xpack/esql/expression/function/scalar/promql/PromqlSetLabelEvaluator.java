/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

/**
 * Hand-written {@link ExpressionEvaluator} for {@link PromqlSetLabel}. It cannot be generated because it hosts a cross-row
 * cache, which the {@code @Evaluator} generator (whose {@code process} body is stateless and per-row) has no place for.
 * <p>
 * The identity-blob rewrite in {@link PromqlSetLabel#rewrite} writes the label into the blob, re-serializing it in canonical
 * sorted order while copying every untouched label's bytes verbatim. It is a
 * pure function of {@code (blob, value)}, and a time series' identity - hence both inputs - is invariant across its time
 * buckets, so the same computation repeats identically once per bucket. To avoid that redundancy the evaluator keeps a
 * <b>1-slot "last-encountered" cache keyed on {@code (blob, value)}</b>, reset per {@link #eval(Page)}: it remembers the
 * previous inputs and their output and reuses the output on a match, otherwise recomputes and stores. Keying on the actual
 * pure-function inputs makes the cache correct by construction - a miss simply recomputes - independent of row ordering. The
 * pass-1 identity seam emits rows tsid-major, so a series' buckets (which share one {@code (blob, value)}) arrive contiguously
 * and hit the slot on every repeat after the first. When the identity block is ordinal-encoded the slot compares the identity
 * ordinal instead of the blob bytes for an O(1) probe.
 */
public final class PromqlSetLabelEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PromqlSetLabelEvaluator.class);

    private final Source source;
    private final ExpressionEvaluator timeseriesBlock;
    private final ExpressionEvaluator valueBlock;
    private final BytesRef dstName;
    private final DriverContext driverContext;
    private Warnings warnings;

    public PromqlSetLabelEvaluator(
        Source source,
        ExpressionEvaluator timeseriesBlock,
        ExpressionEvaluator valueBlock,
        BytesRef dstName,
        DriverContext driverContext
    ) {
        this.source = source;
        this.timeseriesBlock = timeseriesBlock;
        this.valueBlock = valueBlock;
        this.dstName = dstName;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (BytesRefBlock timeseries = (BytesRefBlock) timeseriesBlock.eval(page)) {
            try (BytesRefBlock value = (BytesRefBlock) valueBlock.eval(page)) {
                return eval(page.getPositionCount(), timeseries, value);
            }
        }
    }

    private BytesRefBlock eval(int positionCount, BytesRefBlock timeseries, BytesRefBlock value) {
        String dst = dstName.utf8ToString();
        // Prefer the identity ordinal as the cache probe when the blob is ordinal-encoded (dense repeats at the seam).
        OrdinalBytesRefBlock ordinalBlock = timeseries.asOrdinals();
        IntBlock ordinals = ordinalBlock != null ? ordinalBlock.getOrdinalsBlock() : null;

        try (BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            // 1-slot last-encountered cache, reset per page (these locals). cachedOutput != null marks the slot occupied.
            int cachedOrdinal = -1;
            BytesRef cachedBlob = null;
            BytesRef cachedValue = null;
            BytesRef cachedOutput = null;

            BytesRef blobScratch = new BytesRef();
            BytesRef valueScratch = new BytesRef();

            for (int p = 0; p < positionCount; p++) {
                if (timeseries.isNull(p)) {
                    // No identity to rewrite: nothing to write into.
                    result.appendNull();
                    continue;
                }
                int blobIndex = timeseries.getFirstValueIndex(p);
                BytesRef blob = timeseries.getBytesRef(blobIndex, blobScratch);
                if (value.isNull(p) || value.getValueCount(p) == 0) {
                    // No-op (label_replace no-match): leave the identity untouched, byte-for-byte.
                    result.appendBytesRef(blob);
                    continue;
                }
                BytesRef labelValue = value.getBytesRef(value.getFirstValueIndex(p), valueScratch);

                boolean blobMatches = cachedOutput != null
                    && (ordinals != null ? ordinals.getInt(blobIndex) == cachedOrdinal : blob.bytesEquals(cachedBlob));
                if (blobMatches && labelValue.bytesEquals(cachedValue)) {
                    result.appendBytesRef(cachedOutput);
                    continue;
                }

                try {
                    BytesRef out = PromqlSetLabel.rewrite(blob, dst, labelValue, labelValue.length == 0);
                    result.appendBytesRef(out);
                    cachedOutput = out;
                    cachedValue = BytesRef.deepCopyOf(labelValue);
                    if (ordinals != null) {
                        cachedOrdinal = ordinals.getInt(blobIndex);
                    } else {
                        cachedBlob = BytesRef.deepCopyOf(blob);
                    }
                } catch (IOException e) {
                    warnings().registerException(e);
                    result.appendNull();
                }
            }
            return result.build();
        }
    }

    @Override
    public long baseRamBytesUsed() {
        long baseRamBytesUsed = BASE_RAM_BYTES_USED;
        baseRamBytesUsed += timeseriesBlock.baseRamBytesUsed();
        baseRamBytesUsed += valueBlock.baseRamBytesUsed();
        return baseRamBytesUsed;
    }

    @Override
    public String toString() {
        return "PromqlSetLabelEvaluator["
            + "timeseriesBlock="
            + timeseriesBlock
            + ", valueBlock="
            + valueBlock
            + ", dstName="
            + dstName
            + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(timeseriesBlock, valueBlock);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
        }
        return warnings;
    }

    static final class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final ExpressionEvaluator.Factory timeseriesBlock;
        private final ExpressionEvaluator.Factory valueBlock;
        private final BytesRef dstName;

        Factory(Source source, ExpressionEvaluator.Factory timeseriesBlock, ExpressionEvaluator.Factory valueBlock, BytesRef dstName) {
            this.source = source;
            this.timeseriesBlock = timeseriesBlock;
            this.valueBlock = valueBlock;
            this.dstName = dstName;
        }

        @Override
        public PromqlSetLabelEvaluator get(DriverContext context) {
            return new PromqlSetLabelEvaluator(source, timeseriesBlock.get(context), valueBlock.get(context), dstName, context);
        }

        @Override
        public String toString() {
            return "PromqlSetLabelEvaluator["
                + "timeseriesBlock="
                + timeseriesBlock
                + ", valueBlock="
                + valueBlock
                + ", dstName="
                + dstName
                + "]";
        }
    }
}
