/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class DataStreamFeatureSetUsage extends XPackFeatureSet.Usage {
    private final DataStreamStats streamStats;

    public DataStreamFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.streamStats = new DataStreamStats(input);
    }

    public DataStreamFeatureSetUsage(DataStreamStats stats) {
        super(XPackField.DATA_STREAMS, true, true);
        this.streamStats = stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        streamStats.writeTo(out);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_9_0;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("data_streams", streamStats.totalDataStreamCount);
        builder.field("indices_count", streamStats.indicesBehindDataStream);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return streamStats.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamFeatureSetUsage other = (DataStreamFeatureSetUsage) obj;
        return Objects.equals(streamStats, other.streamStats);
    }

    public static class DataStreamStats implements Writeable {

        private final long totalDataStreamCount;
        private final long indicesBehindDataStream;

        public DataStreamStats(long totalDataStreamCount, long indicesBehindDataStream) {
            this.totalDataStreamCount = totalDataStreamCount;
            this.indicesBehindDataStream = indicesBehindDataStream;
        }

        public DataStreamStats(StreamInput in) throws IOException {
            this.totalDataStreamCount = in.readVLong();
            this.indicesBehindDataStream = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(this.totalDataStreamCount);
            out.writeVLong(this.indicesBehindDataStream);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalDataStreamCount, indicesBehindDataStream);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() != getClass()) {
                return false;
            }
            DataStreamStats other = (DataStreamStats) obj;
            return totalDataStreamCount == other.totalDataStreamCount && indicesBehindDataStream == other.indicesBehindDataStream;
        }
    }
}
