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
import java.util.List;

/**
 * The {@code WITH { ... }} options of an {@code INTO} command. They travel with the plan to every node that writes or
 * merges the results, so each node applies the same write semantics.
 *
 * @param keepGoing when {@code true}, a row the destination refuses is counted as a failure and the query continues;
 *                  when {@code false}, the first refused row fails the query
 */
public record IntoOptions(boolean refresh, boolean keepGoing) implements Writeable {
    public static final String REFRESH = "refresh";
    public static final String KEEP_GOING = "keep_going";

    /** The names of every option, in the order error messages list them. */
    public static final List<String> NAMES = List.of(REFRESH, KEEP_GOING);

    public static final IntoOptions DEFAULT = new IntoOptions(true, false);

    public IntoOptions(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(refresh);
        out.writeBoolean(keepGoing);
    }

    /** Appends each option that differs from {@link #DEFAULT}, so a plan that sets none keeps its existing rendering. */
    public void appendNonDefault(StringBuilder sb) {
        if (refresh != DEFAULT.refresh) {
            sb.append(',').append(REFRESH).append('=').append(refresh);
        }
        if (keepGoing != DEFAULT.keepGoing) {
            sb.append(',').append(KEEP_GOING).append('=').append(keepGoing);
        }
    }
}
