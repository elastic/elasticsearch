/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Simple class to parse and store a flush ID.
 */
public class FlushAcknowledgement extends ToXContentToBytes implements Writeable {
    /**
     * Field Names
     */
    public static final ParseField TYPE = new ParseField("flush");
    public static final ParseField ID = new ParseField("id");

    public static final ConstructingObjectParser<FlushAcknowledgement, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(), a -> new FlushAcknowledgement((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
    }

    private String id;

    public FlushAcknowledgement(String id) {
        this.id = id;
    }

    public FlushAcknowledgement(StreamInput in) throws IOException {
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    public String getId() {
        return id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
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
        return Objects.equals(id, other.id);
    }
}

