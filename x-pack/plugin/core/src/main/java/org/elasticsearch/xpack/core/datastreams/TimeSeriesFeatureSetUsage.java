/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Telemetry for time series data, only time series data streams (TSDS) are tracked. For each TSDS we track:
 * - their time series backing indices
 * - their downsampled backing indices
 * - the downsampled data streams, backing indices and downsampling rounds split by feature (ILM or DLM)
 * - for ILM specifically, we count also the phase in which the downsampling round was configured only for
 * policies used by said data streams
 * {
 *   "time_series": {
 *      "enabled": true,
 *      "available": true,
 *      "data_stream_count": 10,
 *      "index_count": 100,
 *      "downsampling": {
 *         "index_count_per_interval": {
 *           "5m": 5,
 *           "10m": 10,
 *           "1h": 10000
 *        },
 *        "ilm": {
 *          "downsampled_data_stream_count": 8,
 *          "downsampled_index_count": 50,
 *          "rounds_per_data_stream": {
 *            "min": 1,
 *            "max": 3,
 *            "average": 2
 *          },
 *          "phases_in_use": {
 *             "hot": 10,
 *             "warm": 5,
 *             "cold": 10
 *          }
 *        },
 *        "dlm": {
 *          "downsampled_data_stream_count": 8,
 *          "downsampled_index_count": 50,
 *          "rounds_per_data_stream": {
 *            "min": 1,
 *            "max": 3,
 *            "average": 2
 *         }
 *        }
 *      }
 *   }
 * }
 */
public class TimeSeriesFeatureSetUsage extends XPackFeatureUsage {

    private static final TransportVersion TIME_SERIES_TELEMETRY = TransportVersion.fromName("time_series_telemetry");

    private final long timeSeriesDataStreamCount;
    private final long timeSeriesIndexCount;
    private final DownsamplingUsage downsamplingUsage;

    public TimeSeriesFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.timeSeriesDataStreamCount = input.readVLong();
        if (timeSeriesDataStreamCount == 0) {
            timeSeriesIndexCount = 0;
            downsamplingUsage = null;
        } else {
            this.timeSeriesIndexCount = input.readVLong();
            this.downsamplingUsage = input.readOptionalWriteable(DownsamplingUsage::read);
        }
    }

    /**
     * Helper constructor that only requires DLM stats. This can be used when elasticsearch is running in
     * data-stream-lifecycle-only mode. In this mode ILM is not supported, which entails there will be no stats either.
     */
    public TimeSeriesFeatureSetUsage(
        long timeSeriesDataStreamCount,
        long timeSeriesIndexCount,
        DownsamplingFeatureStats dlmDownsamplingStats,
        Map<String, Long> indexCountPerInterval
    ) {
        this(timeSeriesDataStreamCount, timeSeriesIndexCount, null, null, dlmDownsamplingStats, indexCountPerInterval);
    }

    public TimeSeriesFeatureSetUsage(
        long timeSeriesDataStreamCount,
        long timeSeriesIndexCount,
        DownsamplingFeatureStats ilmDownsamplingStats,
        Map<String, Long> phasesUsedInDownsampling,
        DownsamplingFeatureStats dlmDownsamplingStats,
        Map<String, Long> indexCountPerInterval
    ) {
        super(XPackField.TIME_SERIES_DATA_STREAMS, true, true);
        this.timeSeriesDataStreamCount = timeSeriesDataStreamCount;
        if (timeSeriesDataStreamCount == 0) {
            this.timeSeriesIndexCount = 0;
            this.downsamplingUsage = null;
        } else {
            this.timeSeriesIndexCount = timeSeriesIndexCount;
            this.downsamplingUsage = new DownsamplingUsage(
                ilmDownsamplingStats,
                phasesUsedInDownsampling,
                dlmDownsamplingStats,
                indexCountPerInterval
            );
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timeSeriesDataStreamCount);
        if (timeSeriesDataStreamCount > 0) {
            out.writeVLong(timeSeriesIndexCount);
            out.writeOptionalWriteable(downsamplingUsage);
        }

    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TIME_SERIES_TELEMETRY;
    }

    public long getTimeSeriesDataStreamCount() {
        return timeSeriesDataStreamCount;
    }

    public long getTimeSeriesIndexCount() {
        return timeSeriesIndexCount;
    }

    public DownsamplingUsage getDownsamplingUsage() {
        return downsamplingUsage;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("data_stream_count", timeSeriesDataStreamCount);
        if (timeSeriesDataStreamCount > 0) {
            builder.field("index_count", timeSeriesIndexCount);
        }
        if (downsamplingUsage != null) {
            builder.field("downsampling", downsamplingUsage);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSeriesDataStreamCount, timeSeriesIndexCount, downsamplingUsage);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        TimeSeriesFeatureSetUsage other = (TimeSeriesFeatureSetUsage) obj;
        return timeSeriesDataStreamCount == other.timeSeriesDataStreamCount
            && timeSeriesIndexCount == other.timeSeriesIndexCount
            && Objects.equals(downsamplingUsage, other.downsamplingUsage);
    }

    public record DownsamplingUsage(
        DownsamplingFeatureStats ilmDownsamplingStats,
        Map<String, Long> phasesUsedInDownsampling,
        DownsamplingFeatureStats dlmDownsamplingStats,
        Map<String, Long> indexCountPerInterval
    ) implements Writeable, ToXContentObject {

        public static DownsamplingUsage read(StreamInput in) throws IOException {
            DownsamplingFeatureStats ilmDownsamplingStats = in.readOptionalWriteable(DownsamplingFeatureStats::read);
            Map<String, Long> phasesUsedInDownsampling = ilmDownsamplingStats != null
                ? in.readImmutableMap(StreamInput::readString, StreamInput::readVLong)
                : null;
            DownsamplingFeatureStats dlmDownsamplingStats = DownsamplingFeatureStats.read(in);
            Map<String, Long> indexCountPerInterval = in.readImmutableMap(StreamInput::readString, StreamInput::readVLong);
            return new DownsamplingUsage(ilmDownsamplingStats, phasesUsedInDownsampling, dlmDownsamplingStats, indexCountPerInterval);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(ilmDownsamplingStats);
            if (ilmDownsamplingStats != null) {
                out.writeMap(phasesUsedInDownsampling, StreamOutput::writeString, StreamOutput::writeVLong);
            }
            dlmDownsamplingStats.writeTo(out);
            out.writeMap(indexCountPerInterval, StreamOutput::writeString, StreamOutput::writeVLong);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (indexCountPerInterval != null && indexCountPerInterval.isEmpty() == false) {
                builder.startObject("index_count_per_interval");
                for (Map.Entry<String, Long> entry : indexCountPerInterval.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }
            if (ilmDownsamplingStats != null) {
                builder.startObject("ilm");
                ilmDownsamplingStats.toXContent(builder, params);
                builder.field("phases_in_use", phasesUsedInDownsampling);
                builder.endObject();
            }
            if (dlmDownsamplingStats != null) {
                builder.startObject("dlm");
                dlmDownsamplingStats.toXContent(builder, params);
                builder.endObject();
            }
            return builder.endObject();
        }
    }

    public record DownsamplingFeatureStats(long dataStreamsCount, long indexCount, long minRounds, double averageRounds, long maxRounds)
        implements
            Writeable,
            ToXContentFragment {

        static final DownsamplingFeatureStats EMPTY = new DownsamplingFeatureStats(0, 0, 0, 0.0, 0);

        public static DownsamplingFeatureStats read(StreamInput in) throws IOException {
            long dataStreamsCount = in.readVLong();
            if (dataStreamsCount == 0) {
                return EMPTY;
            } else {
                return new DownsamplingFeatureStats(dataStreamsCount, in.readVLong(), in.readVLong(), in.readDouble(), in.readVLong());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(this.dataStreamsCount);
            if (this.dataStreamsCount != 0) {
                out.writeVLong(this.indexCount);
                out.writeVLong(this.minRounds);
                out.writeDouble(this.averageRounds);
                out.writeVLong(this.maxRounds);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("downsampled_data_stream_count", dataStreamsCount);
            if (dataStreamsCount > 0) {
                builder.field("downsampled_index_count", indexCount);
                builder.startObject("rounds_per_data_stream");
                builder.field("min", minRounds);
                builder.field("average", averageRounds);
                builder.field("max", maxRounds);
                builder.endObject();
            }
            return builder;
        }
    }
}
