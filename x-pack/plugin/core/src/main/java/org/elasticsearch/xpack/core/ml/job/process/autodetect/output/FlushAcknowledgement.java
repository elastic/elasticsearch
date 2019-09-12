/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.output;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
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
            TYPE.getPreferredName(), a -> new FlushAcknowledgement((String) a[0], (Date) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                p -> TimeUtils.parseTimeField(p, LAST_FINALIZED_BUCKET_END.getPreferredName()),
                LAST_FINALIZED_BUCKET_END, ObjectParser.ValueType.VALUE);
    }

    private String id;
    private Date lastFinalizedBucketEnd;

    public FlushAcknowledgement(String id, Date lastFinalizedBucketEnd) {
        this.id = id;
        this.lastFinalizedBucketEnd = lastFinalizedBucketEnd;
    }

    public FlushAcknowledgement(StreamInput in) throws IOException {
        id = in.readString();
        lastFinalizedBucketEnd = new Date(in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVLong(lastFinalizedBucketEnd.getTime());
    }

    public String getId() {
        return id;
    }

    public Date getLastFinalizedBucketEnd() {
        return lastFinalizedBucketEnd;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        if (lastFinalizedBucketEnd != null) {
            builder.timeField(LAST_FINALIZED_BUCKET_END.getPreferredName(), LAST_FINALIZED_BUCKET_END.getPreferredName() + "_string",
                    lastFinalizedBucketEnd.getTime());
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

