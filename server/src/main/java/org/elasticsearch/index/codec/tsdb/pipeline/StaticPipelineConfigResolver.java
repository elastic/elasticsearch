/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * A {@link PipelineConfigResolver} that returns a per-field pipeline:
 *
 * <ul>
 *   <li>The {@code @timestamp} field and monotonic long counters
 *       ({@link MetricRole#COUNTER} with {@link PipelineDescriptor.DataType#LONG})
 *       use {@code splitDelta > delta > offset > gcd > bitPack} to recover compression on
 *       TSDB boundary blocks (where {@code _tsid} transitions cause
 *       {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage}
 *       to decline and bit-pack to use {@code log2(time_range)} bits per value).</li>
 *   <li>Double gauges ({@link PipelineDescriptor.DataType#DOUBLE} with
 *       {@link MetricRole#GAUGE}) use {@code alpDouble > delta > offset > gcd > bitPack}
 *       so ALP can convert the IEEE 754 doubles to integer mantissas before the standard
 *       integer transforms compress them further. {@code delta} is included even though
 *       gauges are typically non-monotonic: stages opt out per block, so it costs nothing
 *       on oscillating blocks and shaves bits on any block where the gauge does happen to
 *       run monotonically.</li>
 *   <li>Double counters ({@link PipelineDescriptor.DataType#DOUBLE} with
 *       {@link MetricRole#COUNTER}) use {@code alpDouble > splitDelta > delta > offset >
 *       gcd > bitPack}, mirroring the long-counter routing on top of ALP. The post-ALP
 *       mantissa stream preserves the original counter shape (ALP is order-preserving),
 *       so {@code splitDelta} sees the same {@code _tsid} boundary flips it would on raw
 *       longs and produces the same compaction. {@code kMax} is sized from
 *       {@code blockSize} as {@code clamp(blockSize / 32, 4, 64)} so large blocks with
 *       many resets do not bow out under the default cap.</li>
 *   <li>IP TSDB dimension fields ({@link MappedFieldType#IP} with
 *       {@link FieldContext#isDimension()}) use a
 *       block size of {@code 1024} ({@code blockShift=10}) regardless of the format-level
 *       default. Only {@code blockSize()} of the returned config is used for ordinal
 *       fields; the pipeline stages are never executed. At {@code blockSize=1024},
 *       {@code maxCycleLength=256}, covering low-cardinality dimension cycles that
 *       arise after a {@code _tsid}-sorted merge.</li>
 *   <li>All other fields use the ES819 baseline {@code delta > offset > gcd > bitPack}.</li>
 * </ul>
 *
 * <p>The three known ordinal block sizes ({@code 128}, {@code 512}, and {@code 1024})
 * and the two numeric production block sizes ({@code 128} and {@code 512}, see
 * {@code ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT} and {@code NUMERIC_LARGE_BLOCK_SHIFT})
 * have their {@link PipelineConfig} precomputed at class load for the baseline, split-delta,
 * ALP-double-gauge, and ALP-double-counter variants, so the per-field write path reuses a
 * single instance instead of rebuilding the same builder chain on every call. Unknown
 * block sizes (e.g. those used by unit tests) fall back to a fresh build.
 */
public final class StaticPipelineConfigResolver implements PipelineConfigResolver {

    /** Shared stateless instance; mirrors the {@code INSTANCE} pattern used by stage classes. */
    public static final StaticPipelineConfigResolver INSTANCE = new StaticPipelineConfigResolver();

    // NOTE: matches the literal field name used in IndexSortConfig for the TSDB primary
    // sort. Kept local to avoid pulling cluster.metadata into the codec package.
    private static final String TIMESTAMP_FIELD_NAME = "@timestamp";

    private static final PipelineConfig BLOCK_128 = build(128);
    private static final PipelineConfig BLOCK_512 = build(512);
    private static final PipelineConfig BLOCK_1024 = build(1024);
    private static final PipelineConfig SPLIT_DELTA_BLOCK_128 = buildSplitDelta(128);
    private static final PipelineConfig SPLIT_DELTA_BLOCK_512 = buildSplitDelta(512);
    private static final PipelineConfig ALP_DOUBLE_GAUGE_BLOCK_128 = buildAlpDoubleGauge(128);
    private static final PipelineConfig ALP_DOUBLE_GAUGE_BLOCK_512 = buildAlpDoubleGauge(512);
    private static final PipelineConfig ALP_DOUBLE_COUNTER_BLOCK_128 = buildAlpDoubleCounter(128);
    private static final PipelineConfig ALP_DOUBLE_COUNTER_BLOCK_512 = buildAlpDoubleCounter(512);

    private StaticPipelineConfigResolver() {}

    @Override
    public PipelineConfig resolve(final FieldContext context) {
        if (useSplitDelta(context)) {
            return splitDeltaConfig(context.blockSize());
        }
        if (useAlpDoubleCounter(context)) {
            return alpDoubleCounterConfig(context.blockSize());
        }
        if (useAlpDoubleGauge(context)) {
            return alpDoubleGaugeConfig(context.blockSize());
        }
        if (useOrdinalLargeBlock(context)) {
            return BLOCK_1024;
        }
        return baselineConfig(context.blockSize());
    }

    private static boolean useOrdinalLargeBlock(final FieldContext context) {
        // NOTE: IP dimensions hold a per-host IP, or a per-host list of IPs as the standard OTel
        // collector emits. Dimensions are clustered by the _tsid sort, so that value repeats
        // contiguously within a host: a single IP as a run, a repeated list as a cycle. A larger
        // ordinal block captures both; longer runs amortize per-block overhead and longer cycles
        // stay within maxCycleLength (blockSize / 4). Non-dimension IP fields are not clustered,
        // so they form neither.
        return context.mappedFieldType() == MappedFieldType.IP && context.isDimension();
    }

    private static boolean useSplitDelta(final FieldContext context) {
        if (TIMESTAMP_FIELD_NAME.equals(context.fieldName())) {
            return true;
        }
        return context.dataType() == PipelineDescriptor.DataType.LONG && context.metricRole() == MetricRole.COUNTER;
    }

    private static boolean useAlpDoubleGauge(final FieldContext context) {
        return context.dataType() == PipelineDescriptor.DataType.DOUBLE && context.metricRole() == MetricRole.GAUGE;
    }

    private static boolean useAlpDoubleCounter(final FieldContext context) {
        return context.dataType() == PipelineDescriptor.DataType.DOUBLE && context.metricRole() == MetricRole.COUNTER;
    }

    private static PipelineConfig splitDeltaConfig(final int blockSize) {
        if (blockSize == 128) {
            return SPLIT_DELTA_BLOCK_128;
        }
        if (blockSize == 512) {
            return SPLIT_DELTA_BLOCK_512;
        }
        return buildSplitDelta(blockSize);
    }

    private static PipelineConfig alpDoubleGaugeConfig(final int blockSize) {
        if (blockSize == 128) {
            return ALP_DOUBLE_GAUGE_BLOCK_128;
        }
        if (blockSize == 512) {
            return ALP_DOUBLE_GAUGE_BLOCK_512;
        }
        return buildAlpDoubleGauge(blockSize);
    }

    private static PipelineConfig alpDoubleCounterConfig(final int blockSize) {
        if (blockSize == 128) {
            return ALP_DOUBLE_COUNTER_BLOCK_128;
        }
        if (blockSize == 512) {
            return ALP_DOUBLE_COUNTER_BLOCK_512;
        }
        return buildAlpDoubleCounter(blockSize);
    }

    private static PipelineConfig baselineConfig(final int blockSize) {
        if (blockSize == 128) {
            return BLOCK_128;
        }
        if (blockSize == 512) {
            return BLOCK_512;
        }
        if (blockSize == 1024) {
            return BLOCK_1024;
        }
        return build(blockSize);
    }

    private static PipelineConfig build(final int blockSize) {
        return PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
    }

    private static PipelineConfig buildSplitDelta(final int blockSize) {
        return PipelineConfig.forLongs(blockSize).splitDelta().delta().offset().gcd().bitPack();
    }

    private static PipelineConfig buildAlpDoubleGauge(final int blockSize) {
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage().delta().offset().gcd().bitPack();
    }

    private static PipelineConfig buildAlpDoubleCounter(final int blockSize) {
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage().splitDelta(splitDeltaKMax(blockSize)).delta().offset().gcd().bitPack();
    }

    private static int splitDeltaKMax(final int blockSize) {
        return Math.clamp((long) blockSize / 32, 4, StageSpec.SplitDeltaStage.MAX_K_MAX);
    }
}
