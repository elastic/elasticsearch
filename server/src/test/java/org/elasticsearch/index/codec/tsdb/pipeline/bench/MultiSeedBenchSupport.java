/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.bench;

import org.elasticsearch.core.PathUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

final class MultiSeedBenchSupport {

    private MultiSeedBenchSupport() {}

    // NOTE: 30 deterministic seeds spread across the hash space. Index 0 matches DEFAULT_SEED.
    static final long[] DEFAULT_SEEDS = {
        0x5DEECE66DL,
        0xA1B2C3D4E5F6L,
        0x123456789ABCL,
        0xFEDCBA987654L,
        0x0F1E2D3C4B5AL,
        0x6A7B8C9D0E1FL,
        0xD4C3B2A19080L,
        0x1122334455667L,
        0x8899AABBCCDL,
        0x5566778899AAL,
        0x2233445566778L,
        0xBBCCDDEEFF001L,
        0x7E8F9A0B1C2DL,
        0x3D4E5F607182L,
        0xC1D2E3F40516L,
        0x9081726354A5L,
        0x0A1B2C3D4E5FL,
        0xF0E1D2C3B4A5L,
        0x6789ABCDEF01L,
        0x13579BDF02468L,
        0xACE02468BDF1L,
        0x8642097531ECL,
        0xDB97531FEC86L,
        0x2468ACE13579L,
        0xFDB97531ECA8L,
        0x1357924680BDFL,
        0xECA86420FDB9L,
        0x4B5C6D7E8F90L,
        0xA0B1C2D3E4F5L,
        0x5F6E7D8C9BABL, };

    // NOTE: system property controls how many seeds to use. Default is 30 (all seeds).
    static final String SEED_COUNT_PROPERTY = "pipeline.bench.seed.count";
    static final int DEFAULT_SEED_COUNT = 30;

    static long[] getSeeds() {
        final int count = Integer.parseInt(System.getProperty(SEED_COUNT_PROPERTY, String.valueOf(DEFAULT_SEED_COUNT)));
        if (count < 1 || count > DEFAULT_SEEDS.length) {
            throw new IllegalArgumentException("seed count must be 1.." + DEFAULT_SEEDS.length);
        }
        return Arrays.copyOf(DEFAULT_SEEDS, count);
    }

    // NOTE: sweep tests use this instead of getSeeds(). Falls back to general seed count.
    // Sweep is O(seeds * pipelines * datasets * blockSizes), so a dedicated property allows
    // running sweep at fewer seeds while other tests use the full 30.
    static final String SWEEP_SEED_COUNT_PROPERTY = "pipeline.bench.seed.count.sweep";

    static long[] getSweepSeeds() {
        final String sweepValue = System.getProperty(SWEEP_SEED_COUNT_PROPERTY);
        if (sweepValue != null) {
            final int count = Integer.parseInt(sweepValue);
            if (count < 1 || count > DEFAULT_SEEDS.length) {
                throw new IllegalArgumentException("sweep seed count must be 1.." + DEFAULT_SEEDS.length);
            }
            return Arrays.copyOf(DEFAULT_SEEDS, count);
        }
        return getSeeds();
    }

    static boolean isMultiSeed() {
        return getSeeds().length > 1;
    }

    record AggregatedResult(int median, int p10, int p90) {}

    // NOTE: percentiles use interpolated index (n-1)*p for correct boundary behavior.
    // For 10 seeds: p10 = sorted[floor(9*0.1)] = sorted[0], p90 = sorted[floor(9*0.9)] = sorted[8].
    static AggregatedResult aggregate(final int[] perSeedSizes) {
        final int[] sorted = perSeedSizes.clone();
        Arrays.sort(sorted);
        final int n = sorted.length;
        return new AggregatedResult(
            sorted[n / 2],
            sorted[Math.max(0, (int) Math.floor((n - 1) * 0.1))],
            sorted[Math.min(n - 1, (int) Math.floor((n - 1) * 0.9))]
        );
    }

    // NOTE: returns pipeline index that won (smallest encoded size) for each seed.
    // NOTE: per-seed winner selection with tie-break: size, then stageCount (fewer = better), then name (lexicographic).
    // When stageCounts and names are null, falls back to index order (legacy behaviour).
    static int[] winnerPerSeed(final int[][] perSeedPerPipeline, final int[] stageCounts, final String[] names) {
        final int seedCount = perSeedPerPipeline.length;
        final int[] winners = new int[seedCount];
        for (int s = 0; s < seedCount; s++) {
            int best = 0;
            for (int p = 1; p < perSeedPerPipeline[s].length; p++) {
                final int sizeCmp = Integer.compare(perSeedPerPipeline[s][p], perSeedPerPipeline[s][best]);
                if (sizeCmp < 0) {
                    best = p;
                } else if (sizeCmp == 0 && stageCounts != null) {
                    final int scCmp = Integer.compare(stageCounts[p], stageCounts[best]);
                    if (scCmp < 0) {
                        best = p;
                    } else if (scCmp == 0 && names != null && names[p].compareTo(names[best]) < 0) {
                        best = p;
                    }
                }
            }
            winners[s] = best;
        }
        return winners;
    }

    // NOTE: legacy overload without tie-break metadata (falls back to index order on ties).
    static int[] winnerPerSeed(final int[][] perSeedPerPipeline) {
        return winnerPerSeed(perSeedPerPipeline, null, null);
    }

    // NOTE: counts how many seeds each pipeline won.
    static int[] winCounts(final int[] winners, final int pipelineCount) {
        final int[] counts = new int[pipelineCount];
        for (final int w : winners) {
            counts[w]++;
        }
        return counts;
    }

    // NOTE: bootstrap resamples property. Minimum 10 to avoid degenerate CI.
    static final String BOOTSTRAP_RESAMPLES_PROPERTY = "pipeline.bench.bootstrap.resamples";
    static final int DEFAULT_RESAMPLES = 1000;

    static int getResampleCount() {
        final int count = Integer.parseInt(System.getProperty(BOOTSTRAP_RESAMPLES_PROPERTY, String.valueOf(DEFAULT_RESAMPLES)));
        if (count < 10) {
            throw new IllegalArgumentException("bootstrap resamples must be >= 10, got " + count);
        }
        return count;
    }

    record BootstrapResult(double ciLow, double ciHigh) {}

    // NOTE: bootstrap resampling for 95% CI of delta percentage vs reference.
    // Each resample draws N values with replacement from {0..seedCount-1},
    // computes median(pipeline[resampled]) - median(ref[resampled]) as a percentage of median(ref[resampled]).
    // Paired resampling: same index drawn for ref and pipeline, preserving per-seed pairing.
    static BootstrapResult bootstrap(final int[] refPerSeed, final int[] pipelinePerSeed, final long bootstrapSeed) {
        final int resamples = getResampleCount();
        final int n = refPerSeed.length;
        final Random rng = new Random(bootstrapSeed);
        final double[] deltas = new double[resamples];
        final int[] refSample = new int[n];
        final int[] pipeSample = new int[n];

        for (int r = 0; r < resamples; r++) {
            for (int i = 0; i < n; i++) {
                final int idx = rng.nextInt(n);
                refSample[i] = refPerSeed[idx];
                pipeSample[i] = pipelinePerSeed[idx];
            }
            Arrays.sort(refSample);
            Arrays.sort(pipeSample);
            final int refMedian = refSample[n / 2];
            final int pipeMedian = pipeSample[n / 2];
            deltas[r] = refMedian == 0 ? 0.0 : (double) (pipeMedian - refMedian) * 100.0 / refMedian;
        }
        Arrays.sort(deltas);
        return new BootstrapResult(
            deltas[(int) Math.floor((resamples - 1) * 0.025)],
            deltas[(int) Math.min(resamples - 1, Math.floor((resamples - 1) * 0.975))]
        );
    }

    // ---- Grouping metadata ----

    // NOTE: lightweight metadata for grouping and labeling pipelines in output.
    // dataType and lossy are classification dimensions; stageCount breaks ties
    // in the ranking when two pipelines produce the same encoded size (fewer stages = better).
    // isReference marks the primary baseline (ES87 legacy) -- labeled "(ref)".
    // isPipelineReference marks the secondary baseline (ES87-pipeline) -- labeled "(pipe-ref)".
    // Neither reference participates in winner-stability counting.
    record PipelineMeta(
        PipelineTestUtils.DataType dataType,
        boolean lossy,
        int stageCount,
        boolean isReference,
        boolean isPipelineReference
    ) {}

    // NOTE: a group is one (dataType, lossy) slice of the full result set.
    record ResultGroup(
        String label,
        String filenameSuffix,
        List<String> pipelineNames,
        List<Integer> pipelineIndices,
        List<PipelineMeta> pipelineMetas,
        List<String> dataNames,
        List<Integer> dataIndices
    ) {}

    // NOTE: groups are ordered: double lossless, double lossy,
    // integer lossless, integer lossy. Empty groups are skipped.
    static List<ResultGroup> groupResults(
        final List<String> pipelineNames,
        final List<PipelineMeta> pipelineMetas,
        final List<String> dataNames,
        final List<PipelineTestUtils.DataType> dataTypes
    ) {
        final PipelineTestUtils.DataType[] typeOrder = { PipelineTestUtils.DataType.DOUBLE, PipelineTestUtils.DataType.ANY };
        final String[] typeLabels = { "Double", "Integer" };
        final boolean[] lossOrder = { false, true };
        final String[] lossLabels = { "lossless", "lossy" };

        final List<ResultGroup> groups = new ArrayList<>();
        for (int t = 0; t < typeOrder.length; t++) {
            for (int l = 0; l < lossOrder.length; l++) {
                final PipelineTestUtils.DataType groupType = typeOrder[t];
                final boolean groupLossy = lossOrder[l];

                // NOTE: collect datasets matching this group's data type.
                final List<String> gDataNames = new ArrayList<>();
                final List<Integer> gDataIndices = new ArrayList<>();
                for (int d = 0; d < dataNames.size(); d++) {
                    if (dataTypes.get(d) == groupType
                        || (groupType == PipelineTestUtils.DataType.ANY && dataTypes.get(d) == PipelineTestUtils.DataType.ANY)) {
                        gDataNames.add(dataNames.get(d));
                        gDataIndices.add(d);
                    }
                }
                if (gDataNames.isEmpty()) {
                    continue;
                }

                // NOTE: collect pipelines matching this group. References always included.
                final List<String> gPipeNames = new ArrayList<>();
                final List<Integer> gPipeIndices = new ArrayList<>();
                final List<PipelineMeta> gPipeMetas = new ArrayList<>();
                for (int p = 0; p < pipelineNames.size(); p++) {
                    final PipelineMeta meta = pipelineMetas.get(p);
                    if (meta.isReference() || meta.isPipelineReference()) {
                        gPipeNames.add(pipelineNames.get(p));
                        gPipeIndices.add(p);
                        gPipeMetas.add(meta);
                    } else if (PipelineTestUtils.compatible(meta.dataType(), groupType) && meta.lossy() == groupLossy) {
                        gPipeNames.add(pipelineNames.get(p));
                        gPipeIndices.add(p);
                        gPipeMetas.add(meta);
                    }
                }
                // NOTE: skip group if only references remain (no competing pipelines).
                final long competitorCount = gPipeMetas.stream().filter(m -> !m.isReference() && !m.isPipelineReference()).count();
                if (competitorCount == 0) {
                    continue;
                }

                final String label = typeLabels[t] + " " + lossLabels[l];
                final String suffix = typeLabels[t].toLowerCase(Locale.ROOT) + "-" + lossLabels[l];
                groups.add(new ResultGroup(label, suffix, gPipeNames, gPipeIndices, gPipeMetas, gDataNames, gDataIndices));
            }
        }
        return groups;
    }

    // NOTE: formats results grouped by (dataType, lossy). Each group gets its own
    // leaderboard section and winner stability summary.
    static String formatGroupedResultTable(
        final long[] seeds,
        final List<String> pipelineNames,
        final List<PipelineMeta> pipelineMetas,
        final List<String> dataNames,
        final List<PipelineTestUtils.DataType> dataTypes,
        final int[][][] allResults
    ) {
        final List<ResultGroup> groups = groupResults(pipelineNames, pipelineMetas, dataNames, dataTypes);
        final StringBuilder sb = new StringBuilder();
        for (final ResultGroup group : groups) {
            if (seeds.length == 1) {
                formatSingleSeedGroup(sb, group, allResults);
            } else {
                formatMultiSeedGroup(sb, seeds, group, allResults);
            }
        }
        return sb.toString();
    }

    private static void formatSingleSeedGroup(final StringBuilder sb, final ResultGroup group, final int[][][] allResults) {
        sb.append(String.format(Locale.ROOT, "%n=== %s (1 seed) ===%n", group.label()));

        for (int gd = 0; gd < group.dataNames().size(); gd++) {
            final int d = group.dataIndices().get(gd);
            sb.append(String.format(Locale.ROOT, "%n[%s]%n", group.dataNames().get(gd)));

            // NOTE: find reference size for percentage computation.
            int refSize = -1;
            for (int gp = 0; gp < group.pipelineIndices().size(); gp++) {
                if (group.pipelineMetas().get(gp).isReference()) {
                    refSize = allResults[d][group.pipelineIndices().get(gp)][0];
                    break;
                }
            }
            // NOTE: if no ES87 legacy ref, use pipeline ref.
            if (refSize < 0) {
                for (int gp = 0; gp < group.pipelineIndices().size(); gp++) {
                    if (group.pipelineMetas().get(gp).isPipelineReference()) {
                        refSize = allResults[d][group.pipelineIndices().get(gp)][0];
                        break;
                    }
                }
            }

            final int[][] ranked = rankPipelines(group, d, allResults, 0);
            for (int rank = 0; rank < ranked.length; rank++) {
                final int gp = ranked[rank][0];
                final int size = ranked[rank][1];
                final PipelineMeta meta = group.pipelineMetas().get(gp);
                final String pctStr = formatPercentage(meta, size, refSize);
                sb.append(String.format(Locale.ROOT, "  %d. %-50s %6d bytes %s%n", rank + 1, group.pipelineNames().get(gp), size, pctStr));
            }
        }
    }

    private static void formatMultiSeedGroup(
        final StringBuilder sb,
        final long[] seeds,
        final ResultGroup group,
        final int[][][] allResults
    ) {
        sb.append(String.format(Locale.ROOT, "%n=== %s (%d seeds) ===%n", group.label(), seeds.length));

        // NOTE: collect per-seed winner data for stability analysis.
        // winData[gd][seed] = group-local pipeline index of winner.
        final int[][] winData = new int[group.dataNames().size()][seeds.length];

        for (int gd = 0; gd < group.dataNames().size(); gd++) {
            final int d = group.dataIndices().get(gd);
            sb.append(String.format(Locale.ROOT, "%n[%s] (median bytes, %d seeds)%n", group.dataNames().get(gd), seeds.length));

            // NOTE: compute median for each pipeline in this group.
            final int[] medians = new int[group.pipelineIndices().size()];
            for (int gp = 0; gp < group.pipelineIndices().size(); gp++) {
                final int p = group.pipelineIndices().get(gp);
                final int[] perSeed = new int[seeds.length];
                for (int s = 0; s < seeds.length; s++) {
                    perSeed[s] = allResults[d][p][s];
                }
                medians[gp] = aggregate(perSeed).median();
            }

            // NOTE: find reference median for percentage computation.
            int refMedian = -1;
            int refGp = -1;
            for (int gp = 0; gp < group.pipelineIndices().size(); gp++) {
                if (group.pipelineMetas().get(gp).isReference()) {
                    refMedian = medians[gp];
                    refGp = gp;
                    break;
                }
            }
            if (refMedian < 0) {
                for (int gp = 0; gp < group.pipelineIndices().size(); gp++) {
                    if (group.pipelineMetas().get(gp).isPipelineReference()) {
                        refMedian = medians[gp];
                        refGp = gp;
                        break;
                    }
                }
            }

            // NOTE: compute per-seed winners (excluding references).
            // Tie-break: size first, then stageCount (fewer = better), then name (lexicographic).
            // This matches the ranking comparator so wins are consistent with displayed rank.
            for (int s = 0; s < seeds.length; s++) {
                int bestGp = -1;
                int bestSize = Integer.MAX_VALUE;
                int bestStageCount = Integer.MAX_VALUE;
                String bestName = null;
                for (int gp = 0; gp < group.pipelineIndices().size(); gp++) {
                    final PipelineMeta meta = group.pipelineMetas().get(gp);
                    if (meta.isReference() || meta.isPipelineReference()) continue;
                    final int size = allResults[d][group.pipelineIndices().get(gp)][s];
                    final int sc = meta.stageCount();
                    final String name = group.pipelineNames().get(gp);
                    if (size < bestSize
                        || (size == bestSize && sc < bestStageCount)
                        || (size == bestSize && sc == bestStageCount && (bestName == null || name.compareTo(bestName) < 0))) {
                        bestSize = size;
                        bestStageCount = sc;
                        bestName = name;
                        bestGp = gp;
                    }
                }
                winData[gd][s] = bestGp;
            }

            // NOTE: rank by median size, then stageCount (fewer = better), then name (lexicographic).
            final int[][] ranked = new int[group.pipelineIndices().size()][2];
            for (int gp = 0; gp < ranked.length; gp++) {
                ranked[gp][0] = gp;
                ranked[gp][1] = medians[gp];
            }
            Arrays.sort(ranked, (a, b) -> {
                final int cmp = Integer.compare(a[1], b[1]);
                if (cmp != 0) return cmp;
                final int sc = Integer.compare(group.pipelineMetas().get(a[0]).stageCount(), group.pipelineMetas().get(b[0]).stageCount());
                return sc != 0 ? sc : group.pipelineNames().get(a[0]).compareTo(group.pipelineNames().get(b[0]));
            });

            for (int rank = 0; rank < ranked.length; rank++) {
                final int gp = ranked[rank][0];
                final int median = ranked[rank][1];
                final PipelineMeta meta = group.pipelineMetas().get(gp);
                final String pctStr = formatPercentage(meta, median, refMedian);

                // NOTE: p10/p90 and wins for non-reference pipelines.
                final int p = group.pipelineIndices().get(gp);
                final int[] perSeed = new int[seeds.length];
                for (int s = 0; s < seeds.length; s++) {
                    perSeed[s] = allResults[d][p][s];
                }
                final AggregatedResult agg = aggregate(perSeed);

                if (meta.isReference() || meta.isPipelineReference()) {
                    sb.append(
                        String.format(
                            Locale.ROOT,
                            "  %d. %-50s %6d bytes %s  [p10=%d, p90=%d]%n",
                            rank + 1,
                            group.pipelineNames().get(gp),
                            median,
                            pctStr,
                            agg.p10(),
                            agg.p90()
                        )
                    );
                } else {
                    // NOTE: count wins for this pipeline.
                    int wins = 0;
                    for (int s = 0; s < seeds.length; s++) {
                        if (winData[gd][s] == gp) wins++;
                    }
                    // NOTE: bootstrap CI for non-reference pipelines when we have a ref.
                    String ciStr = "";
                    if (refGp >= 0 && seeds.length >= 10) {
                        final int[] refPerSeed = new int[seeds.length];
                        for (int s = 0; s < seeds.length; s++) {
                            refPerSeed[s] = allResults[d][group.pipelineIndices().get(refGp)][s];
                        }
                        final BootstrapResult ci = bootstrap(refPerSeed, perSeed, DEFAULT_SEEDS[0]);
                        ciStr = String.format(Locale.ROOT, " deltaCI=[%+.1f%%, %+.1f%%]", ci.ciLow(), ci.ciHigh());
                    }
                    sb.append(
                        String.format(
                            Locale.ROOT,
                            "  %d. %-50s %6d bytes %s  [p10=%d, p90=%d, wins=%d/%d]%s%n",
                            rank + 1,
                            group.pipelineNames().get(gp),
                            median,
                            pctStr,
                            agg.p10(),
                            agg.p90(),
                            wins,
                            seeds.length,
                            ciStr
                        )
                    );
                }
            }
        }

        // NOTE: winner stability summary.
        sb.append(String.format(Locale.ROOT, "%n--- Winner stability (%s) ---%n", group.label()));
        for (int gd = 0; gd < group.dataNames().size(); gd++) {
            // NOTE: count wins per group-local pipeline index.
            final int[] counts = new int[group.pipelineIndices().size()];
            for (int s = 0; s < seeds.length; s++) {
                if (winData[gd][s] >= 0) {
                    counts[winData[gd][s]]++;
                }
            }
            // NOTE: find top 2 winners for display.
            int best = -1;
            int bestCount = 0;
            int second = -1;
            int secondCount = 0;
            for (int gp = 0; gp < counts.length; gp++) {
                if (counts[gp] > bestCount) {
                    second = best;
                    secondCount = bestCount;
                    best = gp;
                    bestCount = counts[gp];
                } else if (counts[gp] > secondCount) {
                    second = gp;
                    secondCount = counts[gp];
                }
            }
            if (best < 0) continue;
            final String stability = bestCount == seeds.length ? "stable" : "unstable";
            if (secondCount > 0) {
                sb.append(
                    String.format(
                        Locale.ROOT,
                        "%s:  %s wins %d/%d, %s wins %d/%d (%s)%n",
                        group.dataNames().get(gd),
                        group.pipelineNames().get(best),
                        bestCount,
                        seeds.length,
                        group.pipelineNames().get(second),
                        secondCount,
                        seeds.length,
                        stability
                    )
                );
            } else {
                sb.append(
                    String.format(
                        Locale.ROOT,
                        "%s:  %s wins %d/%d seeds (%s)%n",
                        group.dataNames().get(gd),
                        group.pipelineNames().get(best),
                        bestCount,
                        seeds.length,
                        stability
                    )
                );
            }
        }
    }

    // NOTE: returns ranked array of [groupPipelineIndex, encodedSize] sorted by size, stageCount, name.
    private static int[][] rankPipelines(final ResultGroup group, final int dataIndex, final int[][][] allResults, final int seedIndex) {
        final int[][] ranked = new int[group.pipelineIndices().size()][2];
        for (int gp = 0; gp < ranked.length; gp++) {
            ranked[gp][0] = gp;
            ranked[gp][1] = allResults[dataIndex][group.pipelineIndices().get(gp)][seedIndex];
        }
        Arrays.sort(ranked, (a, b) -> {
            final int cmp = Integer.compare(a[1], b[1]);
            if (cmp != 0) return cmp;
            final int sc = Integer.compare(group.pipelineMetas().get(a[0]).stageCount(), group.pipelineMetas().get(b[0]).stageCount());
            return sc != 0 ? sc : group.pipelineNames().get(a[0]).compareTo(group.pipelineNames().get(b[0]));
        });
        return ranked;
    }

    private static String formatPercentage(final PipelineMeta meta, final int size, final int refSize) {
        if (meta.isReference()) {
            return "(ref)";
        } else if (meta.isPipelineReference()) {
            if (refSize > 0) {
                final double pct = (double) (size - refSize) * 100.0 / refSize;
                return String.format(Locale.ROOT, "(pipe-ref, %+.1f%%)", pct);
            }
            return "(pipe-ref)";
        } else if (refSize > 0) {
            final double pct = (double) (size - refSize) * 100.0 / refSize;
            return String.format(Locale.ROOT, "(%+.1f%%)", pct);
        }
        return "";
    }

    // NOTE: replaces non-alphanumeric chars (except - and _) with -, collapses runs, trims edges.
    static String sanitizeForFilename(final String name) {
        return name.replaceAll("[^a-zA-Z0-9_-]+", "-").replaceAll("-{2,}", "-").replaceAll("^-|-$", "");
    }

    static final String OUTPUT_DIR_PROPERTY = "pipeline.bench.output.dir";

    // NOTE: writes grouped output files when pipeline.bench.output.dir is set.
    // testName is used as file prefix (e.g., "testCompareDoublePipelinesMultiSeed").
    static void writeGroupedOutput(
        final String testName,
        final long[] seeds,
        final List<String> pipelineNames,
        final List<PipelineMeta> pipelineMetas,
        final List<String> dataNames,
        final List<PipelineTestUtils.DataType> dataTypes,
        final int[][][] allResults
    ) {
        final String dir = System.getProperty(OUTPUT_DIR_PROPERTY);
        if (dir == null) {
            return;
        }
        try {
            final Path outDir = PathUtils.get(dir);
            Files.createDirectories(outDir);
            final String fullOutput = formatGroupedResultTable(seeds, pipelineNames, pipelineMetas, dataNames, dataTypes, allResults);
            Files.writeString(
                outDir.resolve(testName + ".txt"),
                fullOutput,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );

            final List<ResultGroup> groups = groupResults(pipelineNames, pipelineMetas, dataNames, dataTypes);
            for (final ResultGroup group : groups) {
                final Path groupFile = outDir.resolve(testName + "-" + group.filenameSuffix() + ".txt");
                Files.writeString(groupFile, fullOutput, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
        } catch (final IOException e) {
            // NOTE: best-effort file write; test still passes if output dir is not writable.
            throw new RuntimeException("Failed to write bench output to " + dir, e);
        }
    }

    // NOTE: writes raw text output when pipeline.bench.output.dir is set.
    static void writeRawOutput(final String filename, final String content) {
        final String dir = System.getProperty(OUTPUT_DIR_PROPERTY);
        if (dir == null) {
            return;
        }
        try {
            final Path outDir = PathUtils.get(dir);
            Files.createDirectories(outDir);
            Files.writeString(outDir.resolve(filename), content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write bench output to " + dir, e);
        }
    }
}
