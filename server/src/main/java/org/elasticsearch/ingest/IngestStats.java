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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IngestStats implements Writeable, ToXContentFragment {
    private final Stats totalStats;
    private final Map<String, Tuple<IngestStats.Stats, List<Tuple<String, IngestStats.Stats>>>> statsPerPipeline;

    /**
     * @param totalStats       - The total stats for Ingest. This is the logically the sum of all pipeline stats,
     *                         and pipeline stats are logically the sum of the processor stats.
     * @param statsPerPipeline - A Map(pipelineId -> Tuple(pipelineStats, List(perProcessorStats))
     *                         where perProcessorStats = Tuple(processorDisplayName, processorStats)
     */
    public IngestStats(Stats totalStats, Map<String, Tuple<IngestStats.Stats, List<Tuple<String, IngestStats.Stats>>>> statsPerPipeline) {
        this.totalStats = totalStats;
        this.statsPerPipeline = statsPerPipeline;
    }

    /**
     * Read from a stream.
     */
    public IngestStats(StreamInput in) throws IOException {
        this.totalStats = new Stats(in);
        int size = in.readVInt();
        this.statsPerPipeline = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String pipelineName = in.readString();
            Stats pipelineStats = new Stats(in);
            int processorsSize = in.readVInt();
            List<Tuple<String, IngestStats.Stats>> processors = new ArrayList<>(processorsSize);
            for (int j = 0; j < processorsSize; j++) {
                String processorName = in.readString();
                processors.add(new Tuple<>(processorName, new Stats(in)));
            }
            statsPerPipeline.put(pipelineName, new Tuple<>(pipelineStats, processors));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVInt(statsPerPipeline.size());
        for (Map.Entry<String, Tuple<Stats, List<Tuple<String, Stats>>>> entry : statsPerPipeline.entrySet()) {
            String pipelineName = entry.getKey();
            Stats pipelineStats = entry.getValue().v1();
            out.writeString(pipelineName);
            pipelineStats.writeTo(out);
            List<Tuple<String, Stats>> processorStats = entry.getValue().v2();
            out.writeVInt(processorStats.size());
            for(Tuple<String, Stats> processorTuple : processorStats){
                String processorName = processorTuple.v1();
                Stats processorStat = processorTuple.v2();
                out.writeString(processorName);
                processorStat.writeTo(out);
            }
        }
    }

    /**
     * @return The accumulated stats for all pipelines
     */
    public Stats getTotalStats() {
        return totalStats;
    }

    /**
     * @return The stats on a per pipeline basis.  A Map(pipelineId -> Tuple(pipelineStats, List(perProcessorStats))
     * where perProcessorStats = Tuple(processorDisplayName, processorStats)
     */
    public Map<String, Tuple<IngestStats.Stats, List<Tuple<String, IngestStats.Stats>>>> getStatsPerPipeline() {
        return statsPerPipeline;
    }

    public IngestStats.Stats getStatsForPipeline(String id) {
        return statsPerPipeline.get(id).v1();
    }

    public List<Tuple<String, Stats>> getProcessorStatsForPipeline(String id) {
        return statsPerPipeline.get(id).v2();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ingest");
        builder.startObject("total");
        totalStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("pipelines");
        for (Map.Entry<String, Tuple<Stats, List<Tuple<String,Stats>>>> entry : statsPerPipeline.entrySet()) {
            builder.startObject(entry.getKey());
            Stats pipelineStats = entry.getValue().v1();
            pipelineStats.toXContent(builder, params);
            List<Tuple<String,Stats>> perProcessorStats = entry.getValue().v2();
            builder.startArray("processors");
            for (Tuple<String,Stats> processorStats : perProcessorStats) {
                builder.startObject();
                builder.startObject(processorStats.v1());
                processorStats.v2().toXContent(builder, params);
                builder.endObject();
                builder.endObject();
            }

            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
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
         *
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
}
