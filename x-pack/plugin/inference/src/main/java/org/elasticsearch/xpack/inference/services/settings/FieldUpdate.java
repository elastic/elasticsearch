/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Represents a single field as supplied (or not) by an inference endpoint update request, preserving the three states the
 * update API must distinguish:
 * <ul>
 *   <li><b>absent</b> - the field was not present in the request; the existing value must be kept.</li>
 *   <li><b>cleared</b> - the field was present with an explicit {@code null}; the value must be reset.</li>
 *   <li><b>set</b> - the field was present with a value; the value must replace the existing one.</li>
 * </ul>
 *
 * <p>Update parsers initialize each updatable field to {@link #absent()} and replace it with {@link #of(Object)} from the
 * field's setter, so a single self-describing value captures all three states without an auxiliary "was set" flag.</p>
 *
 * @param <T> the type of the field value
 */
public final class FieldUpdate<T> {

    private static final FieldUpdate<?> ABSENT = new FieldUpdate<>(false, null);

    private final boolean present;
    @Nullable
    private final T value;

    private FieldUpdate(boolean present, @Nullable T value) {
        this.present = present;
        this.value = value;
    }

    /**
     * The field was not part of the request; the existing value should be kept.
     */
    @SuppressWarnings("unchecked")
    public static <T> FieldUpdate<T> absent() {
        return (FieldUpdate<T>) ABSENT;
    }

    /**
     * The field was present in the request. A {@code null} {@code value} represents an explicit JSON {@code null}, i.e. a
     * request to clear the field.
     */
    public static <T> FieldUpdate<T> of(@Nullable T value) {
        return new FieldUpdate<>(true, value);
    }

    /**
     * Declares an updatable, clearable field on an update parser. This is the single entry point used to make any field honor the
     * tri-state update semantics: a field omitted from the request is left untouched (the setter is never invoked), while a field
     * present with a value or an explicit {@code null} is delivered to {@code setter} wrapped in a {@link FieldUpdate}. It centralizes
     * the {@code VALUE_NULL} handling and the {@code *_OR_NULL} value type so callers cannot accidentally use a non-nullable
     * declaration (which would reject an explicit {@code null} instead of clearing).
     *
     * @param parser       the update parser to register the field on
     * @param setter       receives the parsed {@link FieldUpdate}, typically assigning it to a field on the update target
     * @param reader       reads and (optionally) validates the non-null value from the parser
     * @param field        the field name
     * @param nullableType a {@code *_OR_NULL} value type matching {@code reader} (e.g. {@code INT_OR_NULL}, {@code OBJECT_OR_NULL})
     * @param <V>          the update target type
     * @param <T>          the field value type
     */
    public static <V, T> void declareNullable(
        AbstractObjectParser<V, Void> parser,
        BiConsumer<V, FieldUpdate<T>> setter,
        CheckedFunction<XContentParser, T, IOException> reader,
        ParseField field,
        ObjectParser.ValueType nullableType
    ) {
        parser.declareField(
            (target, value) -> setter.accept(target, FieldUpdate.of(value)),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : reader.apply(p),
            field,
            nullableType
        );
    }

    /**
     * Resolves the effective value after applying the update: {@code currentValue} when the field was absent,
     * {@code clearedValue} when it was explicitly nulled, otherwise the supplied value.
     *
     * @param currentValue the value currently stored on the existing settings
     * @param clearedValue the value to use when the field was explicitly cleared
     * @return the value to store on the updated settings
     */
    public T resolve(T currentValue, T clearedValue) {
        if (present == false) {
            return currentValue;
        }
        return value != null ? value : clearedValue;
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
}
