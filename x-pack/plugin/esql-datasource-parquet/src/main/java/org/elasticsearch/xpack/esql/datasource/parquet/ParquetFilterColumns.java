/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Mints parquet-mr {@link Operators.Column} instances from a physical path, without splitting on
 * {@code '.'}. {@code FilterApi.intColumn(String)} always uses {@code ColumnPath.fromDotString},
 * so a leaf literally named {@code source.ip} is treated as nested {@code source -> ip} and
 * {@code RowGroupFilter} drops every row group. The constructors we need are package-private;
 * {@link MethodHandles#privateLookupIn} reaches them without a split package. Nested structs pass
 * one segment per group ({@code "source", "ip"}); a dotted leaf passes the name as one segment.
 */
final class ParquetFilterColumns {

    private static final MethodHandle INT;
    private static final MethodHandle LONG;
    private static final MethodHandle DOUBLE;
    private static final MethodHandle BOOLEAN;
    private static final MethodHandle BINARY;

    static {
        try {
            INT = constructor(Operators.IntColumn.class);
            LONG = constructor(Operators.LongColumn.class);
            DOUBLE = constructor(Operators.DoubleColumn.class);
            BOOLEAN = constructor(Operators.BooleanColumn.class);
            BINARY = constructor(Operators.BinaryColumn.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static MethodHandle constructor(Class<?> type) throws ReflectiveOperationException {
        var lookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());
        return lookup.findConstructor(type, MethodType.methodType(void.class, ColumnPath.class));
    }

    private ParquetFilterColumns() {}

    static Operators.IntColumn intColumn(String... path) {
        try {
            return (Operators.IntColumn) INT.invokeExact(ColumnPath.get(path));
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static Operators.LongColumn longColumn(String... path) {
        try {
            return (Operators.LongColumn) LONG.invokeExact(ColumnPath.get(path));
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static Operators.DoubleColumn doubleColumn(String... path) {
        try {
            return (Operators.DoubleColumn) DOUBLE.invokeExact(ColumnPath.get(path));
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static Operators.BooleanColumn booleanColumn(String... path) {
        try {
            return (Operators.BooleanColumn) BOOLEAN.invokeExact(ColumnPath.get(path));
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    static Operators.BinaryColumn binaryColumn(String... path) {
        try {
            return (Operators.BinaryColumn) BINARY.invokeExact(ColumnPath.get(path));
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private static RuntimeException rethrow(Throwable e) {
        if (e instanceof Error error) {
            throw error;
        }
        if (e instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new IllegalStateException(e);
    }
}
