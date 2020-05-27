/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

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
import java.util.Objects;

public class MemoryUsage implements Writeable, ToXContentObject {

    public static final String TYPE_VALUE = "analytics_memory_usage";

    public static final ParseField PEAK_USAGE_BYTES = new ParseField("peak_usage_bytes");

    public static final ConstructingObjectParser<MemoryUsage, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<MemoryUsage, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<MemoryUsage, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<MemoryUsage, Void> parser = new ConstructingObjectParser<>(TYPE_VALUE,
            ignoreUnknownFields, a -> new MemoryUsage((String) a[0], (Instant) a[1], (long) a[2]));

        parser.declareString((bucket, s) -> {}, Fields.TYPE);
        parser.declareString(ConstructingObjectParser.constructorArg(), Fields.JOB_ID);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, Fields.TIMESTAMP.getPreferredName()),
            Fields.TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        parser.declareLong(ConstructingObjectParser.constructorArg(), PEAK_USAGE_BYTES);
        return parser;
    }

    private final String jobId;
    /**
     * timestamp may only be null when we construct a zero usage object
     */
    private final Instant timestamp;
    private final long peakUsageBytes;

    /**
     * Creates a zero usage object
     */
    public MemoryUsage(String jobId) {
        this(jobId, null, 0);
    }

    public MemoryUsage(String jobId, Instant timestamp, long peakUsageBytes) {
        this.jobId = Objects.requireNonNull(jobId);
        // We intend to store this timestamp in millis granularity. Thus we're rounding here to ensure
        // internal representation matches toXContent
        this.timestamp = timestamp == null ? null : Instant.ofEpochMilli(
            ExceptionsHelper.requireNonNull(timestamp, Fields.TIMESTAMP).toEpochMilli());
        this.peakUsageBytes = peakUsageBytes;
    }

    public MemoryUsage(StreamInput in) throws IOException {
        jobId = in.readString();
        timestamp = in.readOptionalInstant();
        peakUsageBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeOptionalInstant(timestamp);
        out.writeVLong(peakUsageBytes);
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
            && peakUsageBytes == other.peakUsageBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, peakUsageBytes);
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
}
