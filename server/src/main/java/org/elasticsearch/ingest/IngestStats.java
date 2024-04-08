/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public record IngestStats(Stats totalStats, List<PipelineStat> pipelineStats, Map<String, List<ProcessorStat>> processorStats)
    implements
        Writeable,
        ChunkedToXContent {

    private static final Comparator<PipelineStat> PIPELINE_STAT_COMPARATOR = (p1, p2) -> {
        final Stats p2Stats = p2.stats;
        final Stats p1Stats = p1.stats;
        final int ingestTimeCompare = Long.compare(p2Stats.ingestTimeInMillis, p1Stats.ingestTimeInMillis);
        if (ingestTimeCompare == 0) {
            return Long.compare(p2Stats.ingestCount, p1Stats.ingestCount);
        } else {
            return ingestTimeCompare;
        }
    };

    public static final IngestStats IDENTITY = new IngestStats(Stats.IDENTITY, List.of(), Map.of());

    /**
     * @param totalStats - The total stats for Ingest. This is logically the sum of all pipeline stats,
     *                   and pipeline stats are logically the sum of the processor stats.
     * @param pipelineStats - The stats for a given ingest pipeline.
     * @param processorStats - The per-processor stats for a given pipeline. A map keyed by the pipeline identifier.
     */
    public IngestStats {
        pipelineStats = pipelineStats.stream().sorted(PIPELINE_STAT_COMPARATOR).toList();
    }

    /**
     * Read from a stream.
     */
    public static IngestStats read(StreamInput in) throws IOException {
        var stats = new Stats(in);
        var size = in.readVInt();
        var pipelineStats = new ArrayList<PipelineStat>(size);
        var processorStats = Maps.<String, List<ProcessorStat>>newMapWithExpectedSize(size);

        for (var i = 0; i < size; i++) {
            var pipelineId = in.readString();
            var pipelineStat = new Stats(in);
            pipelineStats.add(new PipelineStat(pipelineId, pipelineStat));
            int processorsSize = in.readVInt();
            var processorStatsPerPipeline = new ArrayList<ProcessorStat>(processorsSize);
            for (var j = 0; j < processorsSize; j++) {
                var processorName = in.readString();
                var processorType = in.readString();
                var processorStat = new Stats(in);
                processorStatsPerPipeline.add(new ProcessorStat(processorName, processorType, processorStat));
            }
            processorStats.put(pipelineId, Collections.unmodifiableList(processorStatsPerPipeline));
        }

        return new IngestStats(stats, pipelineStats, processorStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVInt(pipelineStats.size());
        for (PipelineStat pipelineStat : pipelineStats) {
            out.writeString(pipelineStat.pipelineId());
            pipelineStat.stats().writeTo(out);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.pipelineId());
            if (processorStatsForPipeline == null) {
                out.writeVInt(0);
            } else {
                out.writeCollection(processorStatsForPipeline, (o, processorStat) -> {
                    o.writeString(processorStat.name());
                    o.writeString(processorStat.type());
                    processorStat.stats().writeTo(o);
                });
            }
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(

            Iterators.single((builder, params) -> {
                builder.startObject("ingest");
                builder.startObject("total");
                totalStats.toXContent(builder, params);
                builder.endObject();
                builder.startObject("pipelines");
                return builder;
            }),

            Iterators.flatMap(
                pipelineStats.iterator(),
                pipelineStat -> Iterators.concat(

                    Iterators.single((builder, params) -> {
                        builder.startObject(pipelineStat.pipelineId());
                        pipelineStat.stats().toXContent(builder, params);
                        builder.startArray("processors");
                        return builder;
                    }),

                    Iterators.map(
                        processorStats.getOrDefault(pipelineStat.pipelineId(), List.of()).iterator(),
                        processorStat -> (builder, params) -> {
                            builder.startObject();
                            builder.startObject(processorStat.name());
                            builder.field("type", processorStat.type());
                            builder.startObject("stats");
                            processorStat.stats().toXContent(builder, params);
                            builder.endObject();
                            builder.endObject();
                            builder.endObject();
                            return builder;
                        }
                    ),

                    Iterators.<ToXContent>single((builder, params) -> builder.endArray().endObject())
                )
            ),

            Iterators.<ToXContent>single((builder, params) -> builder.endObject().endObject())
        );
    }

    public static IngestStats merge(IngestStats first, IngestStats second) {
        return new IngestStats(
            Stats.merge(first.totalStats, second.totalStats),
            PipelineStat.merge(first.pipelineStats, second.pipelineStats),
            merge(first.processorStats, second.processorStats)
        );
    }

    static Map<String, List<ProcessorStat>> merge(Map<String, List<ProcessorStat>> first, Map<String, List<ProcessorStat>> second) {
        var totalsPerPipelineProcessor = new HashMap<String, List<ProcessorStat>>();

        first.forEach((pipelineId, stats) -> totalsPerPipelineProcessor.merge(pipelineId, stats, ProcessorStat::merge));
        second.forEach((pipelineId, stats) -> totalsPerPipelineProcessor.merge(pipelineId, stats, ProcessorStat::merge));

        return totalsPerPipelineProcessor;
    }

    public record Stats(long ingestCount, long ingestTimeInMillis, long ingestCurrent, long ingestFailedCount)
        implements
            Writeable,
            ToXContentFragment {

        public static final Stats IDENTITY = new Stats(0, 0, 0, 0);

        /**
         * Read from a stream.
         */
        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(ingestCount);
            out.writeVLong(ingestTimeInMillis);
            out.writeVLong(ingestCurrent);
            out.writeVLong(ingestFailedCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("count", ingestCount);
            builder.humanReadableField("time_in_millis", "time", new TimeValue(ingestTimeInMillis, TimeUnit.MILLISECONDS));
            builder.field("current", ingestCurrent);
            builder.field("failed", ingestFailedCount);
            return builder;
        }

        static Stats merge(Stats first, Stats second) {
            return new Stats(
                first.ingestCount + second.ingestCount,
                first.ingestTimeInMillis + second.ingestTimeInMillis,
                first.ingestCurrent + second.ingestCurrent,
                first.ingestFailedCount + second.ingestFailedCount
            );
        }
    }

    /**
     * Easy conversion from scoped {@link IngestMetric} objects to a serializable Stats objects
     */
    static class Builder {
        private Stats totalStats = null;
        private final List<PipelineStat> pipelineStats = new ArrayList<>();
        private final Map<String, List<ProcessorStat>> processorStats = new HashMap<>();

        Builder addTotalMetrics(IngestMetric totalMetric) {
            assert totalStats == null;
            this.totalStats = totalMetric.createStats();
            return this;
        }

        Builder addPipelineMetrics(String pipelineId, IngestMetric pipelineMetric) {
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineMetric.createStats()));
            return this;
        }

        Builder addProcessorMetrics(String pipelineId, String processorName, String processorType, IngestMetric metric) {
            this.processorStats.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStat(processorName, processorType, metric.createStats()));
            return this;
        }

        IngestStats build() {
            return new IngestStats(totalStats, Collections.unmodifiableList(pipelineStats), Collections.unmodifiableMap(processorStats));
        }
    }

    /**
     * Container for pipeline stats.
     */
    public record PipelineStat(String pipelineId, Stats stats) {
        static List<PipelineStat> merge(List<PipelineStat> first, List<PipelineStat> second) {
            var totalsPerPipeline = new HashMap<String, Stats>();

            first.forEach(ps -> totalsPerPipeline.merge(ps.pipelineId, ps.stats, Stats::merge));
            second.forEach(ps -> totalsPerPipeline.merge(ps.pipelineId, ps.stats, Stats::merge));

            return totalsPerPipeline.entrySet()
                .stream()
                .map(v -> new PipelineStat(v.getKey(), v.getValue()))
                .sorted(PIPELINE_STAT_COMPARATOR)
                .toList();
        }
    }

    /**
     * Container for processor stats.
     */
    public record ProcessorStat(String name, String type, Stats stats) {

        private static List<ProcessorStat> merge(List<ProcessorStat> first, List<ProcessorStat> second) {
            // in the simple case, this amounts to summing up the stats in the first and second and returning
            // a new list of stats that contains the sum. but there are a few not-quite-so-simple cases, too,
            // so this logic is a little bit intricate.

            // total up the stats across both sides
            long firstIngestCountTotal = 0;
            for (ProcessorStat ps : first) {
                firstIngestCountTotal += ps.stats.ingestCount;
            }

            long secondIngestCountTotal = 0;
            for (ProcessorStat ps : second) {
                secondIngestCountTotal += ps.stats.ingestCount;
            }

            // early return in the case of a non-ingest node (the sum of the stats will be zero, so just return the other)
            if (firstIngestCountTotal == 0) {
                return second;
            } else if (secondIngestCountTotal == 0) {
                return first;
            }

            // the list of stats can be different depending on the exact order of application of the cluster states
            // that apply a change to a pipeline -- figure out if they match or not (usually they match!!!)

            // speculative execution of the expected, simple case (where we can merge the processor stats)
            // if we process both lists of stats and everything matches up, we can return the resulting merged list
            if (first.size() == second.size()) { // if the sizes of the lists don't match, then we can skip all this
                boolean match = true;
                var merged = new ArrayList<ProcessorStat>(first.size());
                for (var i = 0; i < first.size(); i++) {
                    ProcessorStat ps1 = first.get(i);
                    ProcessorStat ps2 = second.get(i);
                    if (ps1.name.equals(ps2.name) == false || ps1.type.equals(ps2.type) == false) {
                        match = false;
                        break;
                    } else {
                        merged.add(new ProcessorStat(ps1.name, ps1.type, Stats.merge(ps1.stats, ps2.stats)));
                    }
                }
                if (match) {
                    return merged;
                }
            }

            // speculative execution failed, so we're in the unfortunate case. the lists are different, and they
            // can't be meaningfully merged without more information. note that IngestService#innerUpdatePipelines
            // resets the counts if there's enough variation on an update, so we'll favor the side with the *lower*
            // count as being the 'newest' -- the assumption is that the higher side is just a cluster state
            // application away from itself being reset to zero anyway.
            if (firstIngestCountTotal < secondIngestCountTotal) {
                return first;
            } else {
                return second;
            }
        }
    }
}
