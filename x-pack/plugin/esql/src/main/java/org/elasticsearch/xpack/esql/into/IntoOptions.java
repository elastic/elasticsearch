/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * The {@code WITH { ... }} options of an {@code INTO} command. They travel with the plan to every node that writes or
 * merges the results, so each node applies the same write semantics.
 *
 * @param keepGoing when {@code true}, a row the destination refuses is counted as a failure and the query continues;
 *                  when {@code false}, the first refused row fails the query
 */
public record IntoOptions(boolean refresh, boolean keepGoing) implements Writeable {
    /** The options that users can write in {@code WITH { ... }}. */
    public enum Option {
        REFRESH,
        KEEP_GOING;

        /** The names of every option, in the order that error messages list them. */
        public static final List<String> NAMES = Arrays.stream(values()).map(Option::optionName).toList();

        /** The lower-case name that users write. */
        public String optionName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

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
        List<String> nonDefault = new ArrayList<>();
        if (refresh != DEFAULT.refresh) {
            nonDefault.add(Option.REFRESH.optionName() + "=" + refresh);
        }
        if (keepGoing != DEFAULT.keepGoing) {
            nonDefault.add(Option.KEEP_GOING.optionName() + "=" + keepGoing);
        }
        if (nonDefault.isEmpty() == false) {
            sb.append(',').append(Strings.collectionToCommaDelimitedString(nonDefault));
        }
    }

    /** Starts from {@link #DEFAULT}, so a caller names only the options it changes instead of passing positional booleans. */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean refresh = DEFAULT.refresh;
        private boolean keepGoing = DEFAULT.keepGoing;

        private Builder() {}

        public Builder refresh(boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        public Builder keepGoing(boolean keepGoing) {
            this.keepGoing = keepGoing;
            return this;
        }

        public IntoOptions build() {
            return new IntoOptions(refresh, keepGoing);
        }
    }
}
