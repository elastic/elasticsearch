/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datatree;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A type-safe, JSON-like data value used as an intermediate representation when building audit log entries.
 * <p>
 * The model deliberately separates immutable scalar values ({@link DataNull}, {@link DataBoolean}, {@link DataString},
 * {@link DataLong}, {@link DataDouble}, {@link DataInteger}, {@link DataDecimal}) from mutable containers
 * ({@link DataArray}, {@link DataObject}). It never stores arbitrary {@code Object} values, Java {@code null} (use
 * {@link DataNull#INSTANCE}), or invalid JSON numbers, so that encoders can rely on an exhaustive, well-formed shape.
 * <p>
 * Numbers use the smallest faithful representation: integers and decimals that fit a {@code long}/{@code double} are a
 * {@link DataLong}/{@link DataDouble} (the common subset of JSON and the OTel attribute model), while values outside
 * that range are preserved with arbitrary precision as a {@link DataInteger}/{@link DataDecimal}. No number is ever
 * coerced to a string here; how to render a value a target cannot hold losslessly is left to whichever converter emits
 * the tree.
 */
public sealed interface DataValue permits DataNull, DataBoolean, DataString, DataLong, DataDouble, DataInteger, DataDecimal, DataArray,
    DataObject {

    /**
     * @return this value as a {@link DataObject}
     * @throws IllegalStateException if this value is not an object
     */
    default DataObject requireObject() {
        if (this instanceof DataObject object) {
            return object;
        }
        throw typeError("object");
    }

    /**
     * @return this value as a {@link DataArray}
     * @throws IllegalStateException if this value is not an array
     */
    default DataArray requireArray() {
        if (this instanceof DataArray array) {
            return array;
        }
        throw typeError("array");
    }

    /**
     * @return the string held by this value
     * @throws IllegalStateException if this value is not a string
     */
    default String requireString() {
        if (this instanceof DataString(String value)) {
            return value;
        }
        throw typeError("string");
    }

    /**
     * Builds a consistent {@link IllegalStateException} for a failed type expectation.
     *
     * @param expected human-readable name of the expected type
     * @return the exception to throw
     */
    default IllegalStateException typeError(String expected) {
        return new IllegalStateException("Expected " + expected + " but found " + getClass().getSimpleName());
    }

    /**
     * @return a {@link DataString} for the given value, or {@link DataNull#INSTANCE} when {@code value} is {@code null}
     */
    static DataValue of(String value) {
        return value == null ? DataNull.INSTANCE : new DataString(value);
    }

    static DataValue of(boolean value) {
        return new DataBoolean(value);
    }

    static DataValue of(long value) {
        return new DataLong(value);
    }

    /**
     * @return a {@link DataLong} when {@code value} fits in a signed 64-bit {@code long}, otherwise a
     *         {@link DataInteger} holding the value with arbitrary precision so nothing is lost
     */
    static DataValue of(BigInteger value) {
        try {
            return new DataLong(value.longValueExact());
        } catch (ArithmeticException tooLarge) {
            return new DataInteger(value);
        }
    }

    /**
     * @return a {@link DataDouble} for the given value
     * @throws IllegalArgumentException if {@code value} is not finite (JSON cannot represent NaN or infinities)
     */
    static DataValue of(double value) {
        if (Double.isFinite(value) == false) {
            throw new IllegalArgumentException("Non-finite numbers are not valid JSON");
        }
        return new DataDouble(value);
    }

    /**
     * @return a {@link DataDouble} when {@code value} is exactly representable as a {@code double}, otherwise a
     *         {@link DataDecimal} holding the value with arbitrary precision so nothing is lost
     */
    static DataValue of(BigDecimal value) {
        double asDouble = value.doubleValue();
        if (Double.isFinite(asDouble) && new BigDecimal(asDouble).compareTo(value) == 0) {
            return new DataDouble(asDouble);
        }
        return new DataDecimal(value);
    }

    static DataValue nullValue() {
        return DataNull.INSTANCE;
    }
}
