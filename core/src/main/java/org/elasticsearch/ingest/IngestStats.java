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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IngestStats implements Writeable<IngestStats>, ToXContent {

    public final static IngestStats PROTO = new IngestStats(null, null);

    private final Stats totalStats;
    private final Map<String, Stats> statsPerPipeline;

    public IngestStats(Stats totalStats, Map<String, Stats> statsPerPipeline) {
        this.totalStats = totalStats;
        this.statsPerPipeline = statsPerPipeline;
    }

    /**
     * @return The accumulated stats for all pipelines
     */
    public Stats getTotalStats() {
        return totalStats;
    }

    /**
     * @return The stats on a per pipeline basis
     */
    public Map<String, Stats> getStatsPerPipeline() {
        return statsPerPipeline;
    }

    @Override
    public IngestStats readFrom(StreamInput in) throws IOException {
        Stats totalStats = Stats.PROTO.readFrom(in);
        totalStats.readFrom(in);
        int size = in.readVInt();
        Map<String, Stats> statsPerPipeline = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            Stats stats = Stats.PROTO.readFrom(in);
            statsPerPipeline.put(in.readString(), stats);
            stats.readFrom(in);
        }
        return new IngestStats(totalStats, statsPerPipeline);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVLong(statsPerPipeline.size());
        for (Map.Entry<String, Stats> entry : statsPerPipeline.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ingest");
        builder.startObject("total");
        totalStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("pipelines");
        for (Map.Entry<String, Stats> entry : statsPerPipeline.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static class Stats implements Writeable<Stats>, ToXContent {

        private final static Stats PROTO = new Stats(0, 0, 0, 0);

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
        public Stats readFrom(StreamInput in) throws IOException {
            long ingestCount = in.readVLong();
            long ingestTimeInMillis = in.readVLong();
            long ingestCurrent = in.readVLong();
            long ingestFailedCount = in.readVLong();
            return new Stats(ingestCount, ingestTimeInMillis, ingestCurrent, ingestFailedCount);
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
            builder.timeValueField("time_in_millis", "time", ingestTimeInMillis, TimeUnit.MILLISECONDS);
            builder.field("current", ingestCurrent);
            builder.field("failed", ingestFailedCount);
            return builder;
        }
    }
}
