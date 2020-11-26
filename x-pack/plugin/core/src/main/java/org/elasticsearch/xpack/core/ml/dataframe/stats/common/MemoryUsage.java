/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public class MemoryUsage implements Writeable, ToXContentObject {

    public static final String TYPE_VALUE = "analytics_memory_usage";

    public static final ParseField PEAK_USAGE_BYTES = new ParseField("peak_usage_bytes");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField MEMORY_REESTIMATE_BYTES = new ParseField("memory_reestimate_bytes");

    public static final ConstructingObjectParser<MemoryUsage, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<MemoryUsage, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<MemoryUsage, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<MemoryUsage, Void> parser = new ConstructingObjectParser<>(TYPE_VALUE,
            ignoreUnknownFields, a -> new MemoryUsage((String) a[0], (Instant) a[1], (long) a[2], (Status) a[3], (Long) a[4]));

        parser.declareString((bucket, s) -> {}, Fields.TYPE);
        parser.declareString(ConstructingObjectParser.constructorArg(), Fields.JOB_ID);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, Fields.TIMESTAMP.getPreferredName()),
            Fields.TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        parser.declareLong(ConstructingObjectParser.constructorArg(), PEAK_USAGE_BYTES);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), Status::fromString, STATUS);
        parser.declareLong(ConstructingObjectParser.optionalConstructorArg(), MEMORY_REESTIMATE_BYTES);
        return parser;
    }

    private final String jobId;
    /**
     * timestamp may only be null when we construct a zero usage object
     */
    private final Instant timestamp;
    private final long peakUsageBytes;
    private final Status status;
    @Nullable private final Long memoryReestimateBytes;

    /**
     * Creates a zero usage object
     */
    public MemoryUsage(String jobId) {
        this(jobId, null, 0, null, null);
    }

    public MemoryUsage(String jobId, Instant timestamp, long peakUsageBytes, @Nullable Status status,
                       @Nullable Long memoryReestimateBytes) {
        this.jobId = Objects.requireNonNull(jobId);
        // We intend to store this timestamp in millis granularity. Thus we're rounding here to ensure
        // internal representation matches toXContent
        this.timestamp = timestamp == null ? null : Instant.ofEpochMilli(
            ExceptionsHelper.requireNonNull(timestamp, Fields.TIMESTAMP).toEpochMilli());
        this.peakUsageBytes = peakUsageBytes;
        this.status = status == null ? Status.OK : status;
        this.memoryReestimateBytes = memoryReestimateBytes;
    }

    public MemoryUsage(StreamInput in) throws IOException {
        jobId = in.readString();
        timestamp = in.readOptionalInstant();
        peakUsageBytes = in.readVLong();
        status = Status.readFromStream(in);
        memoryReestimateBytes = in.readOptionalVLong();
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeOptionalInstant(timestamp);
        out.writeVLong(peakUsageBytes);
        status.writeTo(out);
        out.writeOptionalVLong(memoryReestimateBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Fields.TYPE.getPreferredName(), TYPE_VALUE);
            builder.field(Fields.JOB_ID.getPreferredName(), jobId);
        }
        if (timestamp != null) {
            builder.timeField(Fields.TIMESTAMP.getPreferredName(), Fields.TIMESTAMP.getPreferredName() + "_string",
                timestamp.toEpochMilli());
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
        return Objects.equals(jobId, other.jobId)
            && Objects.equals(timestamp, other.timestamp)
            && peakUsageBytes == other.peakUsageBytes
            && Objects.equals(status, other.status)
            && Objects.equals(memoryReestimateBytes, other.memoryReestimateBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, peakUsageBytes, status, memoryReestimateBytes);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public String documentId(String jobId) {
        assert timestamp != null;
        return documentIdPrefix(jobId) + timestamp.toEpochMilli();
    }

    public static String documentIdPrefix(String jobId) {
        return TYPE_VALUE + "_" + jobId + "_";
    }

    public enum Status implements Writeable  {
        OK,
        HARD_LIMIT;

        public static Status fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static Status readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Status.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
