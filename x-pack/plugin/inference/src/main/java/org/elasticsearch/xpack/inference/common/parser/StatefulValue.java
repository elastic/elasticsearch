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

/**
 * This class holds a value of type {@param T} that can be in one of three states: undefined, null, or defined with a non-null value.
 * It provides methods to check the state and retrieve the value if present.
 * <p>
 * Undefined means that the value is not defined aka it was absent in the input
 * Null means that the value is defined but explicitly set to null
 * Present means that the value is defined and not null
 * @param <T> the type of the value
 */
public final class StatefulValue<T> {

    static final IllegalStateException NO_VALUE_PRESENT = new IllegalStateException("No value present");

    private static final StatefulValue<?> UNDEFINED_INSTANCE = new StatefulValue<>(null, false);
    private static final StatefulValue<?> NULL_INSTANCE = new StatefulValue<>(null, true);

    public static <T> StatefulValue<T> undefined() {
        @SuppressWarnings("unchecked")
        var absent = (StatefulValue<T>) UNDEFINED_INSTANCE;
        return absent;
    }

    public static <T> StatefulValue<T> nullInstance() {
        @SuppressWarnings("unchecked")
        var nullInstance = (StatefulValue<T>) NULL_INSTANCE;
        return nullInstance;
    }

    public static <T> StatefulValue<T> of(T value) {
        return new StatefulValue<>(Objects.requireNonNull(value), true);
    }

    public static <T> StatefulValue<T> read(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        var isDefined = in.readBoolean();
        if (isDefined == false) {
            return undefined();
        }

        var isNull = in.readBoolean();
        if (isNull) {
            return nullInstance();
        }

        var value = reader.read(in);
        return of(value);
    }

    public static <T> void write(StreamOutput out, StatefulValue<T> statefulValue, Writeable.Writer<T> writer) throws IOException {
        out.writeBoolean(statefulValue.isDefined);
        if (statefulValue.isDefined) {
            out.writeBoolean(statefulValue.isNull());
            if (statefulValue.isPresent()) {
                writer.write(out, statefulValue.value);
            }
        }
    }

    private final T value;
    private final boolean isDefined;

    private StatefulValue(T value, boolean isDefined) {
        this.value = value;
        this.isDefined = isDefined;
    }

    /**
     * Returns true if the value is not defined, meaning it is absent.
     */
    public boolean isUndefined() {
        return isDefined == false;
    }

    /**
     * Returns true if the value is defined and explicitly set to null.
     */
    public boolean isNull() {
        return isDefined && value == null;
    }

    /**
     * Returns true if the value is defined and not null.
     */
    public boolean isPresent() {
        return isDefined && value != null;
    }

    public T get() {
        if (isPresent() == false) {
            throw NO_VALUE_PRESENT;
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
        return Objects.equals(value, statefulValue.value) && isDefined == statefulValue.isDefined;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, isDefined);
    }
}
