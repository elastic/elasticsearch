/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * This class holds a value of type @{param T} that can be in one of 3 states:
 * - It has a concrete value, or
 * - It is missing, or
 * - It is meant to reset any other when it is composed with it.
 * It is mainly used in template composition to capture the case when the user wished to unset any previous values.
 * @param <T>
 */
public class ResettableValue<T> {
    private static final ResettableValue<?> UNSET = new ResettableValue<>(true, null);
    private static final ResettableValue<?> UNDEFINED = new ResettableValue<>(false, null);
    private static final String SKIP_UNSET_VALUES = "unset";
    private static final Map<String, String> SKIP_RESET_VALUES_PARAMS = Map.of(SKIP_UNSET_VALUES, "true");

    private final T value;
    private final boolean isDefined;

    /**
     * @return the unset state, meaning that this value is explicitly requested to be unset
     */
    public static <T> ResettableValue<T> unset() {
        @SuppressWarnings("unchecked")
        ResettableValue<T> t = (ResettableValue<T>) UNSET;
        return t;
    }

    /**
     * @return the undefined state, meaning that this value has not been specified
     */
    public static <T> ResettableValue<T> undefined() {
        @SuppressWarnings("unchecked")
        ResettableValue<T> t = (ResettableValue<T>) UNDEFINED;
        return t;
    }

    /**
     * Wraps a value, if the value is null, it returns {@link #undefined()}
     */
    public static <T> ResettableValue<T> create(T value) {
        if (value == null) {
            return undefined();
        }
        return new ResettableValue<>(true, value);
    }

    private ResettableValue(boolean isDefined, T value) {
        this.isDefined = isDefined;
        this.value = value;
    }

    /**
     * @return true if the state of this is unset
     */
    public boolean isUnset() {
        return isDefined && value == null;
    }

    /**
     * @return true when the value is defined, either with a concrete value or unset.
     */
    public boolean isDefined() {
        return isDefined;
    }

    /**
     * @return the concrete value when that's defined or null
     */
    public T get() {
        return value;
    }

    /**
     * Writes a single optional explicitly nullable value. This method is in direct relation with the
     * {@link #read(StreamInput, Writeable.Reader)} which reads the respective value. It's the
     * responsibility of the caller to preserve order of the fields and their backwards compatibility.
     *
     * @throws IOException
     */
    static <T> void write(StreamOutput out, ResettableValue<T> value, Writeable.Writer<T> writer) throws IOException {
        out.writeBoolean(value.isDefined);
        if (value.isDefined) {
            out.writeBoolean(value.isUnset());
            if (value.isUnset() == false) {
                writer.write(out, value.get());
            }
        }
    }

    /**
     * Reads a single optional and explicitly nullable value. This method is in direct relation with the
     * {@link #write(StreamOutput, ResettableValue, Writeable.Writer)} which writes the respective value. It's the
     * responsibility of the caller to preserve order of the fields and their backwards compatibility.
     *
     * @throws IOException
     */
    @Nullable
    static <T> ResettableValue<T> read(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        boolean isDefined = in.readBoolean();
        if (isDefined == false) {
            return ResettableValue.undefined();
        }
        boolean shouldReset = in.readBoolean();
        if (shouldReset) {
            return ResettableValue.unset();
        }
        T value = reader.read(in);
        return ResettableValue.create(value);
    }

    /**
     * Gets the value and applies the function {@param f} when the value is not null.
     */
    public <U> U applyAndGet(Function<? super T, ? extends U> f) {
        if (isDefined() == false || isUnset()) {
            return null;
        } else {
            return f.apply(value);
        }
    }

    public <U> ResettableValue<U> map(Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(mapper);
        if (isDefined == false) {
            return ResettableValue.undefined();
        }
        if (isUnset()) {
            return unset();
        }
        return ResettableValue.create(mapper.apply(value));
    }

    /**
     * Applies the {@param mergeFunction} to the current value while respecting the different states:
     * - If the newer value is unset, it returns {@link #undefined()} since it unsets any previous values
     * - If the newer value is undefined, it returns <code>this</code>
     * - If the current value is undefined, it returns {@param other}
     * - otherwise, it merges the two values based on the provided function
     */
    public ResettableValue<T> merge(ResettableValue<T> other, Function<T, T> mergeFunction) {
        if (other.isUnset()) {
            return undefined();
        }
        if (other.isDefined() == false) {
            return this;
        }
        if (this.isDefined() == false || this.isUnset()) {
            return other;
        }
        return this.map(mergeFunction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResettableValue<?> that = (ResettableValue<?>) o;
        return isDefined == that.isDefined && Objects.equals(value, that.value);
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params, String field) throws IOException {
        return toXContent(builder, params, field, Function.identity());
    }

    public <U> XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params, String field, Function<T, U> transformValue)
        throws IOException {
        if (isDefined) {
            if (value != null) {
                builder.field(field, transformValue.apply(value));
            } else if (ResettableValue.shouldDisplayResetValue(params)) {
                builder.nullField(field);
            }
        }
        return builder;
    }

    public static boolean shouldDisplayResetValue(ToXContent.Params params) {
        return params.paramAsBoolean(SKIP_UNSET_VALUES, false) == false;
    }

    public static ToXContent.Params disableResetValues(ToXContent.Params params) {
        return new ToXContent.DelegatingMapParams(SKIP_RESET_VALUES_PARAMS, params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, isDefined);
    }

    @Override
    public String toString() {
        return "ResettableValue{" + "value=" + value + ", isDefined=" + isDefined + '}';
    }
}
