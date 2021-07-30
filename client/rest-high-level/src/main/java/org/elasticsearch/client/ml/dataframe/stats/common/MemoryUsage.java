/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.common;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public class MemoryUsage implements ToXContentObject {

    static final ParseField TIMESTAMP = new ParseField("timestamp");
    static final ParseField PEAK_USAGE_BYTES = new ParseField("peak_usage_bytes");
    static final ParseField STATUS = new ParseField("status");
    static final ParseField MEMORY_REESTIMATE_BYTES = new ParseField("memory_reestimate_bytes");

    public static final ConstructingObjectParser<MemoryUsage, Void> PARSER = new ConstructingObjectParser<>("analytics_memory_usage",
        true, a -> new MemoryUsage((Instant) a[0], (long) a[1], (Status) a[2], (Long) a[3]));

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PEAK_USAGE_BYTES);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), Status::fromString, STATUS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MEMORY_REESTIMATE_BYTES);
    }

    @Nullable
    private final Instant timestamp;
    private final long peakUsageBytes;
    private final Status status;
    private final Long memoryReestimateBytes;

    public MemoryUsage(@Nullable Instant timestamp, long peakUsageBytes, Status status, @Nullable Long memoryReestimateBytes) {
        this.timestamp = timestamp == null ? null : Instant.ofEpochMilli(Objects.requireNonNull(timestamp).toEpochMilli());
        this.peakUsageBytes = peakUsageBytes;
        this.status = status;
        this.memoryReestimateBytes = memoryReestimateBytes;
    }

    @Nullable
    public Instant getTimestamp() {
        return timestamp;
    }

    public long getPeakUsageBytes() {
        return peakUsageBytes;
    }

    public Status getStatus() {
        return status;
    }

    public Long getMemoryReestimateBytes() {
        return memoryReestimateBytes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (timestamp != null) {
            builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.toEpochMilli());
        }
        builder.field(PEAK_USAGE_BYTES.getPreferredName(), peakUsageBytes);
        builder.field(STATUS.getPreferredName(), status);
        if (memoryReestimateBytes != null) {
            builder.field(MEMORY_REESTIMATE_BYTES.getPreferredName(), memoryReestimateBytes);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryUsage other = (MemoryUsage) o;
        return Objects.equals(timestamp, other.timestamp)
            && peakUsageBytes == other.peakUsageBytes
            && Objects.equals(status, other.status)
            && Objects.equals(memoryReestimateBytes, other.memoryReestimateBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, peakUsageBytes, status, memoryReestimateBytes);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(getClass())
            .add(TIMESTAMP.getPreferredName(), timestamp == null ? null : timestamp.getEpochSecond())
            .add(PEAK_USAGE_BYTES.getPreferredName(), peakUsageBytes)
            .add(STATUS.getPreferredName(), status)
            .add(MEMORY_REESTIMATE_BYTES.getPreferredName(), memoryReestimateBytes)
            .toString();
    }

    public enum Status {
        OK,
        HARD_LIMIT;

        public static Status fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
