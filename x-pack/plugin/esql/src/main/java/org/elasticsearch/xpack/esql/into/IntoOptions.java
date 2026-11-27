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
import java.util.Objects;
import java.util.Optional;

/**
 * The {@code WITH { ... }} options of an {@code INTO} command. They travel with the plan to every node that writes or
 * merges the results, so each node applies the same write semantics.
 *
 * @param keepGoing when {@code true}, a row the destination refuses is counted as a failure and the query continues;
 *                  when {@code false}, the first refused row fails the query
 * @param collisions what a writer does with a row whose {@code _id} already exists in the destination
 */
public record IntoOptions(boolean refresh, boolean keepGoing, Collisions collisions) implements Writeable {
    /** The options that users can write in {@code WITH { ... }}. */
    public enum Option {
        REFRESH,
        KEEP_GOING,
        COLLISIONS;

        /** The names of every option, in the order that error messages list them. */
        public static final List<String> NAMES = Arrays.stream(values()).map(Option::optionName).toList();

        /** The lower-case name that users write. */
        public String optionName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum Collisions {
        /** Fails on the first collision. */
        ABORT,
        /** Leaves the existing document, counted in {@code version_conflicts}. */
        KEEP,
        /** Overwrites the existing document, counted in {@code updated}. */
        REPLACE;

        /** The lower-case names users write, in the order error messages list them. */
        public static final List<String> NAMES = Arrays.stream(values()).map(Collisions::optionValue).toList();

        public String optionValue() {
            return name().toLowerCase(Locale.ROOT);
        }

        /** Resolves {@code value} case-insensitively, or returns empty when it names no policy. */
        public static Optional<Collisions> parse(String value) {
            String normalized = value.toLowerCase(Locale.ROOT);
            return Arrays.stream(values()).filter(c -> c.optionValue().equals(normalized)).findFirst();
        }
    }

    public static final IntoOptions DEFAULT = builder().build();

    public IntoOptions {
        Objects.requireNonNull(collisions);
    }

    public IntoOptions(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readBoolean(), in.readEnum(Collisions.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(refresh);
        out.writeBoolean(keepGoing);
        out.writeEnum(collisions);
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
        if (collisions != DEFAULT.collisions) {
            nonDefault.add(Option.COLLISIONS.optionName() + "=" + collisions.optionValue());
        }
        if (nonDefault.isEmpty() == false) {
            sb.append(',').append(Strings.collectionToCommaDelimitedString(nonDefault));
        }
    }

    /** Starts from the default options, so a caller names only the options it changes instead of passing positional booleans. */
    public static Builder builder() {
        return new Builder();
    }

    /** Starts from these options, so a caller names only the options it changes. */
    public Builder toBuilder() {
        return builder().refresh(refresh).keepGoing(keepGoing).collisions(collisions);
    }

    /** The initial values here are the defaults: {@link #DEFAULT} is built from them. */
    public static final class Builder {
        private boolean refresh = true;
        private boolean keepGoing = false;
        private Collisions collisions = Collisions.ABORT;

        private Builder() {}

        public Builder refresh(boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        public Builder keepGoing(boolean keepGoing) {
            this.keepGoing = keepGoing;
            return this;
        }

        public Builder collisions(Collisions collisions) {
            this.collisions = collisions;
            return this;
        }

        public IntoOptions build() {
            return new IntoOptions(refresh, keepGoing, collisions);
        }
    }
}
