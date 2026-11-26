/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * The {@code WITH { ... }} options of an {@code INTO} command. They travel with the plan to every node that writes or
 * merges the results, so each node applies the same write semantics.
 */
public record IntoOptions(boolean refresh) implements Writeable {
    public static final IntoOptions DEFAULT = new IntoOptions(true);

    public IntoOptions(StreamInput in) throws IOException {
        this(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(refresh);
    }

    /** Appends each option that differs from {@link #DEFAULT}, so a plan that sets none keeps its existing rendering. */
    public void appendNonDefault(StringBuilder sb) {
        if (refresh != DEFAULT.refresh) {
            sb.append(",refresh=").append(refresh);
        }
    }
}
