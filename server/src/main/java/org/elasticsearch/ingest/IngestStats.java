/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IngestStats implements Writeable, ToXContentFragment {
    private final Stats totalStats;
    private final List<PipelineStat> pipelineStats;
    private final Map<String, List<ProcessorStat>> processorStats;

    /**
     * @param totalStats - The total stats for Ingest. This is the logically the sum of all pipeline stats,
     *                   and pipeline stats are logically the sum of the processor stats.
     * @param pipelineStats - The stats for a given ingest pipeline.
     * @param processorStats - The per-processor stats for a given pipeline. A map keyed by the pipeline identifier.
     */
    public IngestStats(Stats totalStats, List<PipelineStat> pipelineStats, Map<String, List<ProcessorStat>> processorStats) {
        this.totalStats = totalStats;
        this.pipelineStats = pipelineStats;
        this.processorStats = processorStats;
    }

    /**
     * Read from a stream.
     */
    public IngestStats(StreamInput in) throws IOException {
        this.totalStats = new Stats(in);
        int size = in.readVInt();
        this.pipelineStats = new ArrayList<>(size);
        this.processorStats = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String pipelineId = in.readString();
            Stats pipelineStat = new Stats(in);
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineStat));
            int processorsSize = in.readVInt();
            List<ProcessorStat> processorStatsPerPipeline = new ArrayList<>(processorsSize);
            for (int j = 0; j < processorsSize; j++) {
                String processorName = in.readString();
                Stats processorStat = new Stats(in);
                processorStatsPerPipeline.add(new ProcessorStat(processorName, processorStat));
            }
            this.processorStats.put(pipelineId, processorStatsPerPipeline);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVInt(pipelineStats.size());
        for (PipelineStat pipelineStat : pipelineStats) {
            out.writeString(pipelineStat.getPipelineId());
            pipelineStat.getStats().writeTo(out);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.getPipelineId());
            if (processorStatsForPipeline == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(processorStatsForPipeline.size());
                for (ProcessorStat processorStat : processorStatsForPipeline) {
                    out.writeString(processorStat.getName());
                    processorStat.getStats().writeTo(out);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ingest");
        builder.startObject("total");
        totalStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("pipelines");
        for (PipelineStat pipelineStat : pipelineStats) {
            builder.startObject(pipelineStat.getPipelineId());
            pipelineStat.getStats().toXContent(builder, params);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.getPipelineId());
            builder.startArray("processors");
            if (processorStatsForPipeline != null) {
                for (ProcessorStat processorStat : processorStatsForPipeline) {
                    builder.startObject();
                    builder.startObject(processorStat.getName());
                    processorStat.getStats().toXContent(builder, params);
                    builder.endObject();
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public Stats getTotalStats() {
        return totalStats;
    }

    public List<PipelineStat> getPipelineStats() {
        return pipelineStats;
    }

    public Map<String, List<ProcessorStat>> getProcessorStats() {
        return processorStats;
    }

    public static class Stats implements Writeable, ToXContentFragment {

        private final long ingestCount;
        private final long ingestTimeInMillis;
        private final long ingestCurrent;
        private final long ingestFailedCount;

        public Stats(long ingestCount, long ingestTimeInMillis, long ingestCurrent, long ingestFailedCount) {
            this.ingestCount = ingestCount;
            this.ingestTimeInMillis = ingestTimeInMillis;
            this.ingestCurrent = ingestCurrent;
            this.ingestFailedCount = ingestFailedCount;
        }

        /**
         * Read from a stream.
         */
        public Stats(StreamInput in) throws IOException {
            ingestCount = in.readVLong();
            ingestTimeInMillis = in.readVLong();
            ingestCurrent = in.readVLong();
            ingestFailedCount = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(ingestCount);
            out.writeVLong(ingestTimeInMillis);
            out.writeVLong(ingestCurrent);
            out.writeVLong(ingestFailedCount);
        }

        /**
         * @return The total number of executed ingest preprocessing operations.
         */
        public long getIngestCount() {
            return ingestCount;
        }

        /**
         * @return The total time spent of ingest preprocessing in millis.
         */
        public long getIngestTimeInMillis() {
            return ingestTimeInMillis;
        }

        /**
         * @return The total number of ingest preprocessing operations currently executing.
         */
        public long getIngestCurrent() {
            return ingestCurrent;
        }

        /**
         * @return The total number of ingest preprocessing operations that have failed.
         */
        public long getIngestFailedCount() {
            return ingestFailedCount;
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
        private Stats totalStats;
        private List<PipelineStat> pipelineStats = new ArrayList<>();
        private Map<String, List<ProcessorStat>> processorStats = new HashMap<>();


        Builder addTotalMetrics(IngestMetric totalMetric) {
            this.totalStats = totalMetric.createStats();
            return this;
        }

        Builder addPipelineMetrics(String pipelineId, IngestMetric pipelineMetric) {
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineMetric.createStats()));
            return this;
        }

        Builder addProcessorMetrics(String pipelineId, String processorName, IngestMetric metric) {
            this.processorStats.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStat(processorName, metric.createStats()));
            return this;
        }

        IngestStats build() {
            return new IngestStats(totalStats, Collections.unmodifiableList(pipelineStats),
                Collections.unmodifiableMap(processorStats));
        }
    }

    /**
     * Container for pipeline stats.
     */
    public static class PipelineStat {
        private final String pipelineId;
        private final Stats stats;

        public PipelineStat(String pipelineId, Stats stats) {
            this.pipelineId = pipelineId;
            this.stats = stats;
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public Stats getStats() {
            return stats;
        }
    }

    /**
     * Container for processor stats.
     */
    public static class ProcessorStat {
        private final String name;
        private final Stats stats;

        public ProcessorStat(String name, Stats stats) {
            this.name = name;
            this.stats = stats;
        }

        public String getName() {
            return name;
        }

        public Stats getStats() {
            return stats;
        }
    }
}
