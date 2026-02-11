/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.bench;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.NumericPipelineTestCase;
import org.elasticsearch.index.codec.tsdb.pipeline.bench.PipelineTestUtils.DataType;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

// NOTE: Sweeps block sizes for each double pipeline and reports bytes/value at each size.
// Now multi-seed: each (pipeline, dataset, blockSize) triple is encoded across all seeds and
// the median bytes/value is taken. Uses getSweepSeeds() for dedicated seed count override.
// Block-size constraint: ES87 legacy only appears at block size 128.
public class PipelineBlockSizeSweepTests extends NumericPipelineTestCase {

    private static final int[] BLOCK_SIZES = { 128, 256, 512, 1024, 2048, 4096, 8192 };

    private static final int MAX_BLOCK_SIZE = BLOCK_SIZES[BLOCK_SIZES.length - 1];

    private static final double KNEE_THRESHOLD_PCT = Double.parseDouble(System.getProperty("kneeThreshold", "1.0"));
    private static final double OUTLIER_THRESHOLD_PCT = Double.parseDouble(System.getProperty("outlierThreshold", "1.0"));

    public void testBlockSizeSweep() throws IOException {
        final long[] seeds = MultiSeedBenchSupport.getSweepSeeds();
        final StringBuilder fullOutput = new StringBuilder();

        // NOTE: pre-generate data per seed. fullValuesPerSeed[dataIndex][seedIndex] = long[MAX_BLOCK_SIZE].
        final List<String> dataSourceNames = new ArrayList<>();
        final List<DataType> dataSourceTypes = new ArrayList<>();
        final int dataCount;
        {
            // NOTE: use first seed to discover dataset names/types.
            final var doubleFactories = NumericDataGenerators.seededDoubleDataSources();
            for (final var dsf : doubleFactories) {
                dataSourceNames.add(dsf.name());
                dataSourceTypes.add(DataType.DOUBLE);
            }
            dataCount = dataSourceNames.size();
        }

        final long[][][] fullValuesPerSeed = new long[dataCount][seeds.length][];
        for (int s = 0; s < seeds.length; s++) {
            final long seed = seeds[s];
            final var doubleFactories = NumericDataGenerators.seededDoubleDataSources();
            int d = 0;
            for (final var dsf : doubleFactories) {
                final double[] doubles = dsf.generator().apply(MAX_BLOCK_SIZE, seed);
                fullValuesPerSeed[d][s] = NumericDataGenerators.doublesToSortableLongs(doubles);
                d++;
            }
        }

        final int steps = BLOCK_SIZES.length - 1;

        // NOTE: ES87 legacy baseline at block size 128, multi-seeded. Median bytes/value.
        final double[] es87MedianBpv = new double[dataCount];
        {
            final int es87BlockSize = 128;
            for (int d = 0; d < dataCount; d++) {
                final int[] perSeedSize = new int[seeds.length];
                for (int s = 0; s < seeds.length; s++) {
                    final TSDBDocValuesEncoder es87Encoder = new TSDBDocValuesEncoder(es87BlockSize);
                    final long[] values = Arrays.copyOf(fullValuesPerSeed[d][s], es87BlockSize);
                    final byte[] buffer = new byte[es87BlockSize * Long.BYTES + 256];
                    final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
                    es87Encoder.encode(values.clone(), out);
                    perSeedSize[s] = out.getPosition();
                }
                es87MedianBpv[d] = (double) MultiSeedBenchSupport.aggregate(perSeedSize).median() / es87BlockSize;
            }
        }

        // NOTE: ES87-pipeline baseline at each block size, multi-seeded.
        // es87PipelineBpv[blockSizeIndex][dataIndex] = median bytes/value.
        final double[][] es87PipelineBpv = new double[BLOCK_SIZES.length][dataCount];
        for (int b = 0; b < BLOCK_SIZES.length; b++) {
            final int bs = BLOCK_SIZES[b];
            for (int d = 0; d < dataCount; d++) {
                final PipelineTestUtils.PipelineDef baselineDef = PipelineTestUtils.es87PipelineBaselineForDoubles();
                final int[] perSeedSize = new int[seeds.length];
                for (int s = 0; s < seeds.length; s++) {
                    final NumericEncoder encoder = baselineDef.factory().apply(bs);
                    final var enc = encoder.newBlockEncoder();
                    final long[] values = Arrays.copyOf(fullValuesPerSeed[d][s], bs);
                    final byte[] buffer = new byte[bs * Long.BYTES * 4];
                    final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
                    enc.encode(values.clone(), values.length, out);
                    perSeedSize[s] = out.getPosition();
                }
                es87PipelineBpv[b][d] = (double) MultiSeedBenchSupport.aggregate(perSeedSize).median() / bs;
            }
        }

        final List<PipelineTestUtils.PipelineDef> pipelineDefs = PipelineTestUtils.doublePipelines();
        PipelineTestUtils.assertNoAnyType(pipelineDefs);

        // Selection matrix: track best bytes/value and block size per pipeline per dataset.
        final double[][] selectionBpv = new double[pipelineDefs.size()][dataCount];
        final int[][] selectionBs = new int[pipelineDefs.size()][dataCount];
        final boolean[][] selectionCompatible = new boolean[pipelineDefs.size()][dataCount];

        for (int pipelineIndex = 0; pipelineIndex < pipelineDefs.size(); pipelineIndex++) {
            final PipelineTestUtils.PipelineDef pf = pipelineDefs.get(pipelineIndex);
            final String pipelineName = pf.name();

            final StringBuilder sb = new StringBuilder();
            sb.append(
                String.format(
                    Locale.ROOT,
                    "%nBlock-size sweep | pipeline: %s (%s) | seeds: %d%n%n",
                    pipelineName,
                    pf.maxError() > 0 ? "lossy" : "lossless",
                    seeds.length
                )
            );

            final List<List<Double>> stepImprovements = new ArrayList<>();
            for (int s = 0; s < steps; s++) {
                stepImprovements.add(new ArrayList<>());
            }

            final int[] kneeCounts = new int[BLOCK_SIZES.length];
            final List<String> outliers = new ArrayList<>();
            final List<String> incompatible = new ArrayList<>();

            for (int d = 0; d < dataCount; d++) {
                final String dsName = dataSourceNames.get(d);
                final DataType dsType = dataSourceTypes.get(d);
                if (PipelineTestUtils.compatible(pf.dataType(), dsType) == false) {
                    incompatible.add(dsName);
                    selectionCompatible[pipelineIndex][d] = false;
                    sb.append(String.format(Locale.ROOT, "[%s] n/a%n", dsName));
                    continue;
                }
                selectionCompatible[pipelineIndex][d] = true;

                final boolean isZstd = PipelineTestUtils.isZstdPipeline(pf);
                final double[] bytesPerValue = new double[BLOCK_SIZES.length];
                Arrays.fill(bytesPerValue, Double.NaN);
                final double[] stepPct = new double[steps];
                Arrays.fill(stepPct, Double.NaN);

                for (int b = 0; b < BLOCK_SIZES.length; b++) {
                    final int bs = BLOCK_SIZES[b];
                    if (isZstd && bs < PipelineTestUtils.ZSTD_MIN_BLOCK_SIZE) {
                        continue;
                    }

                    // NOTE: multi-seed encode per (pipeline, dataset, blockSize).
                    final int[] perSeedSize = new int[seeds.length];
                    for (int s = 0; s < seeds.length; s++) {
                        final NumericEncoder encoder = pf.factory().apply(bs);
                        final var enc = encoder.newBlockEncoder();
                        final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
                        final var dec = decoder.newBlockDecoder();

                        final long[] values = Arrays.copyOf(fullValuesPerSeed[d][s], bs);
                        final byte[] buffer = new byte[bs * Long.BYTES * 4];
                        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
                        enc.encode(values.clone(), values.length, out);
                        perSeedSize[s] = out.getPosition();

                        final long[] decoded = new long[bs];
                        dec.decode(decoded, new ByteArrayDataInput(buffer, 0, out.getPosition()));
                        if (pf.maxError() > 0) {
                            PipelineTestUtils.assertArrayEqualsWithTolerance(
                                pipelineName + " bs=" + bs + " seed=" + seeds[s] + " round-trip failed for " + dsName,
                                values,
                                decoded,
                                pf.maxError(),
                                dsType
                            );
                        } else {
                            assertArrayEquals(
                                pipelineName + " bs=" + bs + " seed=" + seeds[s] + " round-trip failed for " + dsName,
                                values,
                                decoded
                            );
                        }
                    }
                    bytesPerValue[b] = (double) MultiSeedBenchSupport.aggregate(perSeedSize).median() / bs;
                }

                for (int s = 0; s < steps; s++) {
                    if (Double.isNaN(bytesPerValue[s]) || Double.isNaN(bytesPerValue[s + 1])) {
                        continue;
                    }
                    stepPct[s] = (bytesPerValue[s + 1] - bytesPerValue[s]) / bytesPerValue[s] * 100.0;
                    stepImprovements.get(s).add(stepPct[s]);
                }

                int kneeIdx = BLOCK_SIZES.length - 1;
                for (int s = 0; s < steps; s++) {
                    if (Double.isNaN(stepPct[s])) {
                        continue;
                    }
                    if (Math.abs(stepPct[s]) < KNEE_THRESHOLD_PCT) {
                        kneeIdx = s;
                        break;
                    }
                }
                kneeCounts[kneeIdx]++;

                for (int s = 0; s < steps; s++) {
                    if (Double.isNaN(stepPct[s]) == false && stepPct[s] > OUTLIER_THRESHOLD_PCT) {
                        outliers.add(
                            String.format(Locale.ROOT, "%s: %d->%d = %+.1f%%", dsName, BLOCK_SIZES[s], BLOCK_SIZES[s + 1], stepPct[s])
                        );
                    }
                }

                double bestBpv = Double.MAX_VALUE;
                int bestBsIdx = -1;
                for (int b = 0; b < BLOCK_SIZES.length; b++) {
                    if (Double.isNaN(bytesPerValue[b]) == false && bytesPerValue[b] < bestBpv) {
                        bestBpv = bytesPerValue[b];
                        bestBsIdx = b;
                    }
                }
                selectionBpv[pipelineIndex][d] = bestBpv;
                selectionBs[pipelineIndex][d] = bestBsIdx >= 0 ? BLOCK_SIZES[bestBsIdx] : 0;

                sb.append(String.format(Locale.ROOT, "[%s] raw=%.1f bytes/value:", dsName, 8.0));
                for (int b = 0; b < BLOCK_SIZES.length; b++) {
                    if (Double.isNaN(bytesPerValue[b])) {
                        sb.append(String.format(Locale.ROOT, " %d=n/a", BLOCK_SIZES[b]));
                    } else if (b == 0 || Double.isNaN(bytesPerValue[b - 1])) {
                        sb.append(String.format(Locale.ROOT, " %d=%.3f", BLOCK_SIZES[b], bytesPerValue[b]));
                    } else {
                        sb.append(String.format(Locale.ROOT, ", %d=%.3f (%+.1f%%)", BLOCK_SIZES[b], bytesPerValue[b], stepPct[b - 1]));
                    }
                }
                sb.append("\n");
            }

            // -- Summary --
            sb.append(
                String.format(
                    Locale.ROOT,
                    "%n--- Summary (knee threshold: %.1f%%, outlier threshold: %.1f%%, seeds: %d) ---%n%n",
                    KNEE_THRESHOLD_PCT,
                    OUTLIER_THRESHOLD_PCT,
                    seeds.length
                )
            );
            if (incompatible.isEmpty() == false) {
                sb.append(String.format(Locale.ROOT, "Incompatible datasets: %d%n", incompatible.size()));
                for (final String name : incompatible) {
                    sb.append("  ").append(name).append("\n");
                }
                sb.append("\n");
            }

            sb.append("Preferred block size (knee):\n");
            for (int b = 0; b < BLOCK_SIZES.length; b++) {
                if (kneeCounts[b] > 0) {
                    final String label = b < steps
                        ? String.format(
                            Locale.ROOT,
                            "  bs=%-5d (<%.0f%% gain to %d)",
                            BLOCK_SIZES[b],
                            KNEE_THRESHOLD_PCT,
                            BLOCK_SIZES[b + 1]
                        )
                        : String.format(Locale.ROOT, "  bs=%-5d (still improving at largest)", BLOCK_SIZES[b]);
                    sb.append(String.format(Locale.ROOT, "%s: %d datasets%n", label, kneeCounts[b]));
                }
            }

            sb.append("\nMedian improvement per step:\n");
            for (int s = 0; s < steps; s++) {
                final double med = median(stepImprovements.get(s));
                sb.append(String.format(Locale.ROOT, "  %d->%d: %+.2f%%%n", BLOCK_SIZES[s], BLOCK_SIZES[s + 1], med));
            }

            sb.append(String.format(Locale.ROOT, "%nOutliers (bytes/value increased by >%.1f%% on doubling): ", OUTLIER_THRESHOLD_PCT));
            if (outliers.isEmpty()) {
                sb.append("none\n");
            } else {
                sb.append(String.format(Locale.ROOT, "%d found%n", outliers.size()));
                for (String o : outliers) {
                    sb.append("  ").append(o).append("\n");
                }
            }

            logger.info(sb.toString());
            fullOutput.append(sb);
        }

        // Selection matrix: split by lossiness (double only now).
        // NOTE: reference is block-size-aware. At bs=128: ES87 legacy. At other sizes: ES87-pipeline.
        final boolean[] lossyFlags = { false, true };
        final String[] lossLabels = { "Lossless", "Lossy" };

        for (int l = 0; l < lossyFlags.length; l++) {
            final boolean filterLossy = lossyFlags[l];

            final StringBuilder matrix = new StringBuilder();
            matrix.append(
                String.format(
                    Locale.ROOT,
                    "%n=== Selection matrix: Double %s (%d seeds) ===%n",
                    lossLabels[l].toLowerCase(Locale.ROOT),
                    seeds.length
                )
            );
            matrix.append("* ref at 128=ES87; ref at other sizes=ES87-pipeline\n\n");
            matrix.append(
                String.format(Locale.ROOT, "%-45s %-50s %5s %11s %s%n", "Dataset", "Best pipeline", "BS", "Bytes/value", "vs ref at BS")
            );

            boolean anyRow = false;
            for (int d = 0; d < dataCount; d++) {
                int bestIdx = -1;
                double bestBpv = Double.MAX_VALUE;
                int bestBsIdx = -1;
                for (int p = 0; p < pipelineDefs.size(); p++) {
                    final boolean pipelineLossy = pipelineDefs.get(p).maxError() > 0.0;
                    if (selectionCompatible[p][d] && pipelineLossy == filterLossy && selectionBpv[p][d] < bestBpv) {
                        bestBpv = selectionBpv[p][d];
                        bestIdx = p;
                        bestBsIdx = selectionBs[p][d];
                    }
                }

                if (bestIdx < 0) {
                    continue;
                }

                // NOTE: block-size-aware reference. At bs=128: ES87 legacy. Otherwise: ES87-pipeline at that bs.
                final int bsIndex = Arrays.binarySearch(BLOCK_SIZES, bestBsIdx);
                final double refBpv;
                final String refLabel;
                if (bestBsIdx == 128) {
                    refBpv = es87MedianBpv[d];
                    refLabel = "ES87";
                } else {
                    refBpv = bsIndex >= 0 ? es87PipelineBpv[bsIndex][d] : es87PipelineBpv[0][d];
                    refLabel = "ES87-pipeline";
                }
                final String deltaStr = refBpv == 0.0
                    ? "n/a"
                    : String.format(Locale.ROOT, "%+.1f%% vs %s", (bestBpv - refBpv) / refBpv * 100.0, refLabel);
                matrix.append(
                    String.format(
                        Locale.ROOT,
                        "%-40s %-60s %5d %11.3f %s%n",
                        dataSourceNames.get(d),
                        pipelineDefs.get(bestIdx).name(),
                        bestBsIdx,
                        bestBpv,
                        deltaStr
                    )
                );
                anyRow = true;
            }

            if (anyRow == false) {
                matrix.append("  (no compatible pipelines)\n");
            }

            logger.info(matrix.toString());
            fullOutput.append(matrix);
        }

        // Safe default recommendation.
        final StringBuilder rec = new StringBuilder();
        rec.append(String.format(Locale.ROOT, "%n=== Safe default recommendation (%d seeds) ===%n", seeds.length));
        rec.append("Pipeline with best median delta vs ref, across all compatible datasets.\n");
        rec.append("* ref at 128=ES87; ref at other sizes=ES87-pipeline\n");

        for (int l = 0; l < lossyFlags.length; l++) {
            final boolean filterLossy = lossyFlags[l];
            final String groupLabel = "Double " + lossLabels[l].toLowerCase(Locale.ROOT);

            int bestPipeline = -1;
            double bestMedian = Double.MAX_VALUE;
            double bestWorst = Double.NaN;

            for (int p = 0; p < pipelineDefs.size(); p++) {
                final boolean pipelineLossy = pipelineDefs.get(p).maxError() > 0.0;
                if (pipelineLossy != filterLossy) {
                    continue;
                }

                final List<Double> deltas = new ArrayList<>();
                for (int d = 0; d < dataCount; d++) {
                    if (selectionCompatible[p][d] == false) {
                        continue;
                    }
                    // NOTE: block-size-aware delta. Compare best bpv at its optimal bs vs ref at that bs.
                    final int optimalBs = selectionBs[p][d];
                    final int bsIndex = Arrays.binarySearch(BLOCK_SIZES, optimalBs);
                    final double refBpv;
                    if (optimalBs == 128) {
                        refBpv = es87MedianBpv[d];
                    } else {
                        refBpv = bsIndex >= 0 ? es87PipelineBpv[bsIndex][d] : es87PipelineBpv[0][d];
                    }
                    final double deltaPct = refBpv == 0.0 ? 0.0 : (selectionBpv[p][d] - refBpv) / refBpv * 100.0;
                    deltas.add(deltaPct);
                }

                if (deltas.isEmpty()) {
                    continue;
                }

                final double med = median(deltas);
                if (med < bestMedian) {
                    bestMedian = med;
                    bestPipeline = p;
                    bestWorst = deltas.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
                }
            }

            rec.append(String.format(Locale.ROOT, "%n[%s]%n", groupLabel));
            if (bestPipeline < 0) {
                rec.append("  (no compatible pipelines)\n");
            } else {
                rec.append(
                    String.format(
                        Locale.ROOT,
                        "  Recommended: %s%n  Median delta vs ref: %+.2f%%%n  Worst regression vs ref: %+.2f%%%n",
                        pipelineDefs.get(bestPipeline).name(),
                        bestMedian,
                        bestWorst
                    )
                );
            }
        }

        logger.info(rec.toString());
        fullOutput.append(rec);
        MultiSeedBenchSupport.writeRawOutput("block-size-sweep.txt", fullOutput.toString());
    }

    private static double median(final List<Double> values) {
        final double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        final int n = sorted.length;
        if (n == 0) return 0.0;
        return n % 2 == 0 ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0 : sorted[n / 2];
    }
}
