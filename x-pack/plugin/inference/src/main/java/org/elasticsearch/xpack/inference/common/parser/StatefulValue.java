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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;

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

    static final NoSuchElementException NO_VALUE_PRESENT = new NoSuchElementException("No value present");

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

    /**
     * Declares an updatable, clearable field on an update parser, capturing the field as a {@link StatefulValue} that honours the
     * tri-state update semantics. A field omitted from the request leaves the target untouched (the setter is never invoked, so the
     * field keeps its initial {@link #undefined()} value), a field present with a value is delivered as {@link #of(Object)}, and a
     * field present with an explicit {@code null} (or for which {@code reader} yields {@code null}, e.g. an empty object) is delivered
     * as {@link #nullInstance()}. It centralizes the {@code VALUE_NULL} handling and the {@code *_OR_NULL} value type so callers
     * cannot accidentally use a non-nullable declaration (which would reject an explicit {@code null} instead of clearing).
     *
     * @param parser       the update parser to register the field on
     * @param setter       receives the parsed {@link StatefulValue}, typically assigning it to a field on the update target
     * @param reader       reads and (optionally) validates the value from the parser; a {@code null} result is treated as a clear
     * @param field        the field name
     * @param nullableType a {@code *_OR_NULL} value type matching {@code reader} (e.g. {@code INT_OR_NULL}, {@code OBJECT_OR_NULL})
     * @param <V>          the update target type
     * @param <T>          the field value type
     */
    public static <V, T> void declareNullable(
        AbstractObjectParser<V, Void> parser,
        BiConsumer<V, StatefulValue<T>> setter,
        CheckedFunction<XContentParser, T, IOException> reader,
        ParseField field,
        ObjectParser.ValueType nullableType
    ) {
        parser.declareField(
            (target, value) -> setter.accept(target, value == null ? nullInstance() : of(value)),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : reader.apply(p),
            field,
            nullableType
        );
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

    /**
     * Resolves the effective value after applying the update: {@code currentValue} when the field was {@link #isUndefined() absent},
     * {@code clearedValue} when it was explicitly {@link #isNull() nulled}, otherwise the present value.
     *
     * <p>Note this differs from {@link #orElse(Object)}, which collapses the absent and null states to the same fallback; this method
     * keeps them distinct so an omitted field can keep the current value while an explicit {@code null} resets to a separate default.</p>
     *
     * @param currentValue the value currently stored on the existing settings
     * @param clearedValue the value to use when the field was explicitly cleared
     * @return the value to store on the updated settings
     */
    public T resolve(T currentValue, T clearedValue) {
        if (isUndefined()) {
            return currentValue;
        }
        return isPresent() ? value : clearedValue;
    }

    /**
     * Convenience for fields whose cleared state is simply {@code null}.
     *
     * @param currentValue the value currently stored on the existing settings
     * @return the value to store on the updated settings
     */
    public T resolve(T currentValue) {
        return resolve(currentValue, null);
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
