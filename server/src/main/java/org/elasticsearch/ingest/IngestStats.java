/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public record IngestStats(
    Stats totalStats,
    List<PipelineStat> pipelineStats,
    Map<ProjectId, Map<String, List<ProcessorStat>>> processorStats
) implements Writeable, ChunkedToXContent {

    private static final Comparator<PipelineStat> PIPELINE_STAT_COMPARATOR = Comparator.comparingLong(
        (PipelineStat p) -> p.stats.ingestTimeInMillis
    ).thenComparingLong((PipelineStat p) -> p.stats.ingestCount).thenComparingLong((PipelineStat p) -> p.byteStats.bytesProduced);

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
        // while reading the processors, we're going to encounter identical name and type strings *repeatedly*
        // it's advantageous to discard the endless copies of the same strings and canonical-ize them to keep our
        // heap usage under control. note: this map is key to key, because of the limitations of the set interface.
        final Map<String, String> namesAndTypesCache = new HashMap<>();

        var stats = readStats(in);
        var size = in.readVInt();
        if (stats == Stats.IDENTITY && size == 0) {
            return IDENTITY;
        }
        var pipelineStats = new ArrayList<PipelineStat>(size);
        Map<ProjectId, Map<String, List<ProcessorStat>>> processorStats = new HashMap<>();

        for (var i = 0; i < size; i++) {
            ProjectId projectId = in.getTransportVersion().onOrAfter(TransportVersions.NODES_STATS_SUPPORTS_MULTI_PROJECT)
                ? ProjectId.readFrom(in)
                // We will not have older nodes in a multi-project cluster, so we can assume that everything is in the default project.
                : Metadata.DEFAULT_PROJECT_ID;
            var pipelineId = in.readString();
            var pipelineStat = readStats(in);
            var byteStat = in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0) ? readByteStats(in) : ByteStats.IDENTITY;
            pipelineStats.add(new PipelineStat(projectId, pipelineId, pipelineStat, byteStat));
            int processorsSize = in.readVInt();
            var processorStatsPerPipeline = new ArrayList<ProcessorStat>(processorsSize);
            for (var j = 0; j < processorsSize; j++) {
                var processorName = in.readString();
                var processorType = in.readString();
                var processorStat = readStats(in);
                // pass these name and type through the local names and types cache to canonical-ize them
                processorName = namesAndTypesCache.computeIfAbsent(processorName, Function.identity());
                processorType = namesAndTypesCache.computeIfAbsent(processorType, Function.identity());
                processorStatsPerPipeline.add(new ProcessorStat(processorName, processorType, processorStat));
            }
            processorStats.computeIfAbsent(projectId, k -> new HashMap<>()).put(pipelineId, unmodifiableList(processorStatsPerPipeline));
        }

        return new IngestStats(stats, pipelineStats, unmodifiableMap(processorStats));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVInt(pipelineStats.size());
        for (PipelineStat pipelineStat : pipelineStats) {
            if (out.getTransportVersion().onOrAfter(TransportVersions.NODES_STATS_SUPPORTS_MULTI_PROJECT)) {
                pipelineStat.projectId().writeTo(out);
            }
            out.writeString(pipelineStat.pipelineId());
            pipelineStat.stats().writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                pipelineStat.byteStats().writeTo(out);
            }
            List<ProcessorStat> processorStatsForPipeline = processorStats.getOrDefault(pipelineStat.projectId(), Map.of())
                .get(pipelineStat.pipelineId());
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
                        String key = outerParams.paramAsBoolean(NodeStats.MULTI_PROJECT_ENABLED_XCONTENT_PARAM_KEY, false)
                            ? pipelineStat.projectId() + "/" + pipelineStat.pipelineId()
                            : pipelineStat.pipelineId();
                        builder.startObject(key);
                        pipelineStat.stats().toXContent(builder, params);
                        pipelineStat.byteStats().toXContent(builder, params);
                        builder.startArray("processors");
                        return builder;
                    }),

                    Iterators.map(
                        processorStats.getOrDefault(pipelineStat.projectId(), Map.of())
                            .getOrDefault(pipelineStat.pipelineId(), List.of())
                            .iterator(),
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
                    Iterators.single((builder, params) -> builder.endArray().endObject())
                )
            ),
            Iterators.single((builder, params) -> builder.endObject().endObject())
        );
    }

    public static IngestStats merge(IngestStats first, IngestStats second) {
        return new IngestStats(
            Stats.merge(first.totalStats, second.totalStats),
            PipelineStat.merge(first.pipelineStats, second.pipelineStats),
            merge(first.processorStats, second.processorStats)
        );
    }

    static Map<ProjectId, Map<String, List<ProcessorStat>>> merge(
        Map<ProjectId, Map<String, List<ProcessorStat>>> first,
        Map<ProjectId, Map<String, List<ProcessorStat>>> second
    ) {
        Map<ProjectId, Map<String, List<ProcessorStat>>> totals = new HashMap<>();
        first.forEach((projectId, statsByPipeline) -> totals.merge(projectId, statsByPipeline, IngestStats::innerMerge));
        second.forEach((projectId, statsByPipeline) -> totals.merge(projectId, statsByPipeline, IngestStats::innerMerge));
        return totals;
    }

    private static Map<String, List<ProcessorStat>> innerMerge(
        Map<String, List<ProcessorStat>> first,
        Map<String, List<ProcessorStat>> second
    ) {
        var totalsPerPipelineProcessor = new HashMap<String, List<ProcessorStat>>();

        first.forEach((pipelineId, stats) -> totalsPerPipelineProcessor.merge(pipelineId, stats, ProcessorStat::merge));
        second.forEach((pipelineId, stats) -> totalsPerPipelineProcessor.merge(pipelineId, stats, ProcessorStat::merge));

        return totalsPerPipelineProcessor;
    }

    /**
     * Read {@link Stats} from a stream.
     */
    private static Stats readStats(StreamInput in) throws IOException {
        long ingestCount = in.readVLong();
        long ingestTimeInMillis = in.readVLong();
        long ingestCurrent = in.readVLong();
        long ingestFailedCount = in.readVLong();
        if (ingestCount == 0 && ingestTimeInMillis == 0 && ingestCurrent == 0 && ingestFailedCount == 0) {
            return Stats.IDENTITY;
        } else {
            return new Stats(ingestCount, ingestTimeInMillis, ingestCurrent, ingestFailedCount);
        }
    }

    public record Stats(long ingestCount, long ingestTimeInMillis, long ingestCurrent, long ingestFailedCount)
        implements
            Writeable,
            ToXContentFragment {

        public static final Stats IDENTITY = new Stats(0, 0, 0, 0);

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
        private final Map<ProjectId, Map<String, List<ProcessorStat>>> processorStats = new HashMap<>();

        Builder addTotalMetrics(IngestMetric totalMetric) {
            assert totalStats == null;
            this.totalStats = totalMetric.createStats();
            return this;
        }

        Builder addPipelineMetrics(ProjectId projectId, String pipelineId, IngestPipelineMetric ingestPipelineMetrics) {
            this.pipelineStats.add(
                new PipelineStat(projectId, pipelineId, ingestPipelineMetrics.createStats(), ingestPipelineMetrics.createByteStats())
            );
            return this;
        }

        Builder addProcessorMetrics(
            ProjectId projectId,
            String pipelineId,
            String processorName,
            String processorType,
            IngestMetric metric
        ) {
            this.processorStats.computeIfAbsent(projectId, k -> new HashMap<>())
                .computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStat(processorName, processorType, metric.createStats()));
            return this;
        }

        IngestStats build() {
            return new IngestStats(totalStats, unmodifiableList(pipelineStats), unmodifiableMap(processorStats));
        }
    }

    /**
     * Container for pipeline stats.
     */
    public record PipelineStat(ProjectId projectId, String pipelineId, Stats stats, ByteStats byteStats) {
        static List<PipelineStat> merge(List<PipelineStat> first, List<PipelineStat> second) {
            record MergeKey(ProjectId projectId, String pipelineId) {}

            var totalsPerPipeline = new HashMap<MergeKey, PipelineStat>();

            first.forEach(ps -> totalsPerPipeline.merge(new MergeKey(ps.projectId, ps.pipelineId), ps, PipelineStat::merge));
            second.forEach(ps -> totalsPerPipeline.merge(new MergeKey(ps.projectId, ps.pipelineId), ps, PipelineStat::merge));

            return totalsPerPipeline.entrySet()
                .stream()
                .map(v -> new PipelineStat(v.getKey().projectId(), v.getKey().pipelineId(), v.getValue().stats, v.getValue().byteStats))
                .sorted(PIPELINE_STAT_COMPARATOR)
                .toList();
        }

        private static PipelineStat merge(PipelineStat first, PipelineStat second) {
            assert first.projectId.equals(second.projectId) : "Can only merge stats from the same project";
            assert first.pipelineId.equals(second.pipelineId) : "Can only merge stats from the same pipeline";
            return new PipelineStat(
                first.projectId,
                first.pipelineId,
                Stats.merge(first.stats, second.stats),
                ByteStats.merge(first.byteStats, second.byteStats)
            );
        }
    }

    static ByteStats readByteStats(StreamInput in) throws IOException {
        long bytesIngested = in.readVLong();
        long bytesProduced = in.readVLong();
        if (bytesProduced == 0L && bytesIngested == 0L) {
            return ByteStats.IDENTITY;
        }
        return new ByteStats(bytesIngested, bytesProduced);
    }

    /**
     * Container for ingested byte stats
     */
    public record ByteStats(long bytesIngested, long bytesProduced) implements Writeable, ToXContentFragment {

        public static final ByteStats IDENTITY = new ByteStats(0L, 0L);

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(bytesIngested);
            out.writeVLong(bytesProduced);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(
                "ingested_as_first_pipeline_in_bytes",
                "ingested_as_first_pipeline",
                ByteSizeValue.ofBytes(bytesIngested)
            );
            builder.humanReadableField(
                "produced_as_first_pipeline_in_bytes",
                "produced_as_first_pipeline",
                ByteSizeValue.ofBytes(bytesProduced)
            );
            return builder;
        }

        static ByteStats merge(ByteStats first, ByteStats second) {
            if (first == IDENTITY) {
                return second;
            } else if (second == IDENTITY) {
                return first;
            }
            return new ByteStats((first.bytesIngested + second.bytesIngested), first.bytesProduced + second.bytesProduced);
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
