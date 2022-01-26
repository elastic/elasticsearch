/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.output;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Simple class to parse and store a flush ID.
 */
public class FlushAcknowledgement implements ToXContentObject, Writeable {
    /**
     * Field Names
     */
    public static final ParseField TYPE = new ParseField("flush");
    public static final ParseField ID = new ParseField("id");
    public static final ParseField LAST_FINALIZED_BUCKET_END = new ParseField("last_finalized_bucket_end");

    public static final ConstructingObjectParser<FlushAcknowledgement, Void> PARSER = new ConstructingObjectParser<>(
        TYPE.getPreferredName(),
        a -> new FlushAcknowledgement((String) a[0], (Long) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LAST_FINALIZED_BUCKET_END);
    }

    private final String id;
    private final Instant lastFinalizedBucketEnd;

    public FlushAcknowledgement(String id, Long lastFinalizedBucketEndMs) {
        this.id = id;
        // The C++ passes 0 when last finalized bucket end is not available, so treat 0 as null
        this.lastFinalizedBucketEnd = (lastFinalizedBucketEndMs != null && lastFinalizedBucketEndMs > 0)
            ? Instant.ofEpochMilli(lastFinalizedBucketEndMs)
            : null;
    }

    public FlushAcknowledgement(String id, Instant lastFinalizedBucketEnd) {
        this.id = id;
        // Round to millisecond accuracy to ensure round-tripping via XContent results in an equal object
        long epochMillis = (lastFinalizedBucketEnd != null) ? lastFinalizedBucketEnd.toEpochMilli() : 0;
        this.lastFinalizedBucketEnd = (epochMillis > 0) ? Instant.ofEpochMilli(epochMillis) : null;
    }

    public FlushAcknowledgement(StreamInput in) throws IOException {
        id = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            lastFinalizedBucketEnd = in.readOptionalInstant();
        } else {
            long epochMillis = in.readVLong();
            // Older versions will be storing zero when the desired behaviour was null
            lastFinalizedBucketEnd = (epochMillis > 0) ? Instant.ofEpochMilli(epochMillis) : null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeOptionalInstant(lastFinalizedBucketEnd);
        } else {
            // Older versions cannot tolerate null on the wire even though the rest of the class is designed to cope with null
            long epochMillis = (lastFinalizedBucketEnd != null) ? lastFinalizedBucketEnd.toEpochMilli() : 0;
            out.writeVLong(epochMillis);
        }
    }

    public String getId() {
        return id;
    }

    public Instant getLastFinalizedBucketEnd() {
        return lastFinalizedBucketEnd;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        if (lastFinalizedBucketEnd != null) {
            builder.timeField(
                LAST_FINALIZED_BUCKET_END.getPreferredName(),
                LAST_FINALIZED_BUCKET_END.getPreferredName() + "_string",
                lastFinalizedBucketEnd.toEpochMilli()
            );
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, lastFinalizedBucketEnd);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FlushAcknowledgement other = (FlushAcknowledgement) obj;
        return Objects.equals(id, other.id) && Objects.equals(lastFinalizedBucketEnd, other.lastFinalizedBucketEnd);
    }
}
