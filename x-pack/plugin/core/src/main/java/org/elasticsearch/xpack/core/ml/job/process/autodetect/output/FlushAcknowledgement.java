/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.output;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
            TYPE.getPreferredName(), a -> new FlushAcknowledgement((String) a[0], (Long) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LAST_FINALIZED_BUCKET_END);
    }

    private final String id;
    private final Instant lastFinalizedBucketEnd;

    public FlushAcknowledgement(String id, Long lastFinalizedBucketEndMs) {
        this.id = id;
        // The C++ passes 0 when last finalized bucket end is not available, so treat 0 as null
        this.lastFinalizedBucketEnd =
            (lastFinalizedBucketEndMs != null && lastFinalizedBucketEndMs > 0) ? Instant.ofEpochMilli(lastFinalizedBucketEndMs) : null;
    }

    public FlushAcknowledgement(String id, Instant lastFinalizedBucketEnd) {
        this.id = id;
        // Round to millisecond accuracy to ensure round-tripping via XContent results in an equal object
        long epochMillis = (lastFinalizedBucketEnd != null) ? lastFinalizedBucketEnd.toEpochMilli() : 0;
        this.lastFinalizedBucketEnd = (epochMillis > 0) ? Instant.ofEpochMilli(epochMillis) : null;
    }

    public FlushAcknowledgement(StreamInput in) throws IOException {
        id = in.readString();
        lastFinalizedBucketEnd = in.readOptionalInstant();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalInstant(lastFinalizedBucketEnd);
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
            builder.timeField(LAST_FINALIZED_BUCKET_END.getPreferredName(), LAST_FINALIZED_BUCKET_END.getPreferredName() + "_string",
                    lastFinalizedBucketEnd.toEpochMilli());
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
        return Objects.equals(id, other.id) &&
                Objects.equals(lastFinalizedBucketEnd, other.lastFinalizedBucketEnd);
    }
}
