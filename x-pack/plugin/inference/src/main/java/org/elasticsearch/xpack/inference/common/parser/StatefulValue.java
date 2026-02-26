/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public final class StatefulValue<T> {

    private enum State {
        ABSENT,
        NULL,
        VALUE
    }

    private static final StatefulValue<?> ABSENT_INSTANCE = new StatefulValue<>(null, State.ABSENT);
    private static final StatefulValue<?> NULL_INSTANCE = new StatefulValue<>(null, State.NULL);

    public static <T> StatefulValue<T> absent() {
        @SuppressWarnings("unchecked")
        var absent = (StatefulValue<T>) ABSENT_INSTANCE;
        return absent;
    }

    public static <T> StatefulValue<T> nullInstance() {
        @SuppressWarnings("unchecked")
        var nullInstance = (StatefulValue<T>) NULL_INSTANCE;
        return nullInstance;
    }

    public static <T> StatefulValue<T> of(T value) {
        return new StatefulValue<>(Objects.requireNonNull(value), State.VALUE);
    }

    public static <T> StatefulValue<T> read(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        var state = in.readEnum(State.class);
        if (state == State.ABSENT) {
            return absent();
        } else if (state == State.NULL) {
            return nullInstance();
        } else {
            return of(reader.read(in));
        }
    }

    public static <T> void write(StreamOutput out, StatefulValue<T> presence, Writeable.Writer<T> writer) throws IOException {
        out.writeEnum(presence.state);
        if (presence.state == State.VALUE) {
            writer.write(out, presence.value);
        }
    }

    private final T value;
    private final State state;

    private StatefulValue(T value, State state) {
        this.value = value;
        this.state = Objects.requireNonNull(state);
    }

    public boolean isAbsent() {
        return state == State.ABSENT;
    }

    public boolean isNull() {
        return state == State.NULL;
    }

    public boolean isPresent() {
        return state == State.VALUE;
    }

    public T get() {
        if (state != State.VALUE) {
            throw new IllegalStateException("No value present");
        }
        return value;
    }

    public T orElse(T other) {
        return isPresent() ? value : other;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        StatefulValue<?> statefulValue = (StatefulValue<?>) o;
        return Objects.equals(value, statefulValue.value) && state == statefulValue.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, state);
    }
}
