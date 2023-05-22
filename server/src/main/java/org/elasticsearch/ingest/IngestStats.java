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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public record IngestStats(Stats totalStats, List<PipelineStat> pipelineStats, Map<String, List<ProcessorStat>> processorStats)
    implements
        Writeable,
        ChunkedToXContent {

    /**
     * @param totalStats - The total stats for Ingest. This is logically the sum of all pipeline stats,
     *                   and pipeline stats are logically the sum of the processor stats.
     * @param pipelineStats - The stats for a given ingest pipeline.
     * @param processorStats - The per-processor stats for a given pipeline. A map keyed by the pipeline identifier.
     */
    public IngestStats {
        pipelineStats = pipelineStats.stream().sorted((p1, p2) -> {
            final IngestStats.Stats p2Stats = p2.stats;
            final IngestStats.Stats p1Stats = p1.stats;
            final int ingestTimeCompare = Long.compare(p2Stats.ingestTimeInMillis, p1Stats.ingestTimeInMillis);
            if (ingestTimeCompare == 0) {
                return Long.compare(p2Stats.ingestCount, p1Stats.ingestCount);
            } else {
                return ingestTimeCompare;
            }
        }).toList();
    }

    /**
     * Read from a stream.
     */
    public IngestStats(StreamInput in) throws IOException {
        this(new Stats(in), readPipelineStats(in));
    }

    IngestStats(Stats stats, Tuple<List<PipelineStat>, Map<String, List<ProcessorStat>>> tuple) {
        this(stats, tuple.v1(), tuple.v2());
    }

    private static Tuple<List<PipelineStat>, Map<String, List<ProcessorStat>>> readPipelineStats(StreamInput in) throws IOException {
        var size = in.readVInt();
        var pipelineStats = new ArrayList<PipelineStat>(size);
        var processorStats = Maps.<String, List<ProcessorStat>>newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String pipelineId = in.readString();
            Stats pipelineStat = new Stats(in);
            pipelineStats.add(new PipelineStat(pipelineId, pipelineStat));
            int processorsSize = in.readVInt();
            List<ProcessorStat> processorStatsPerPipeline = new ArrayList<>(processorsSize);
            for (int j = 0; j < processorsSize; j++) {
                String processorName = in.readString();
                String processorType = in.readString();
                Stats processorStat = new Stats(in);
                processorStatsPerPipeline.add(new ProcessorStat(processorName, processorType, processorStat));
            }
            processorStats.put(pipelineId, Collections.unmodifiableList(processorStatsPerPipeline));
        }

        return Tuple.tuple(Collections.unmodifiableList(pipelineStats), Collections.unmodifiableMap(processorStats));
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

                    Iterators.flatMap(
                        processorStats.getOrDefault(pipelineStat.pipelineId(), List.of()).iterator(),
                        processorStat -> Iterators.<ToXContent>single((builder, params) -> {
                            builder.startObject();
                            builder.startObject(processorStat.name());
                            builder.field("type", processorStat.type());
                            builder.startObject("stats");
                            processorStat.stats().toXContent(builder, params);
                            builder.endObject();
                            builder.endObject();
                            builder.endObject();
                            return builder;
                        })
                    ),

                    Iterators.<ToXContent>single((builder, params) -> builder.endArray().endObject())
                )
            ),

            Iterators.<ToXContent>single((builder, params) -> builder.endObject().endObject())
        );
    }

    public record Stats(long ingestCount, long ingestTimeInMillis, long ingestCurrent, long ingestFailedCount)
        implements
            Writeable,
            ToXContentFragment {

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
    public record PipelineStat(String pipelineId, Stats stats) {}

    /**
     * Container for processor stats.
     */
    public record ProcessorStat(String name, String type, Stats stats) {}
}
