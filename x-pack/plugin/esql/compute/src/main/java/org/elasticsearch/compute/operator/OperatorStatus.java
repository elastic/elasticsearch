/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Status of an {@link Operator}.
 *
 * @param operator String representation of the {@link Operator}.
 * @param status Status as reported by the {@link Operator}.
 */
public record OperatorStatus(String operator, @Nullable Operator.Status status) implements Writeable, ToXContentObject {

    public static OperatorStatus readFrom(StreamInput in) throws IOException {
        return new OperatorStatus(in.readString(), in.readOptionalNamedWriteable(Operator.Status.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(operator);
        out.writeOptionalNamedWriteable(status != null && status.supportsVersion(out.getTransportVersion()) ? status : null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("operator", operator);
        if (status != null) {
            builder.field("status", status);
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * The number of documents found by this operator. Most operators
     * don't find documents and will return {@code 0} here.
     */
    public long documentsFound() {
        if (status == null) {
            return 0;
        }
        return status.documentsFound();
    }

    /**
     * The number of values loaded by this operator. Most operators
     * don't load values and will return {@code 0} here.
     */
    public long valuesLoaded() {
        if (status == null) {
            return 0;
        }
        return status.valuesLoaded();
    }
}
