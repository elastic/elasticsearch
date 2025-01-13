/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

// To make things simpler, we're only dealing with a single INSIST parameter for now.
public record InsistParameters(String identifier) implements NamedWriteable {
    @Override
    public String getWriteableName() {
        return "InsistParameters";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(identifier);
    }

    public static InsistParameters readFrom(StreamInput in) throws IOException {
        return new InsistParameters(in.readString());
    }
}
