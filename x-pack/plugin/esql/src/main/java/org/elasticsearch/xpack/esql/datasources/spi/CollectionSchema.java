/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * A repeated structure in a file's schema: a child relation of the file's rows, whose rows are the elements and whose
 * columns are the element's leaves. Only a list of structures is one.
 *
 * @param path        the collection's dotted name in the file's schema, as the query names it
 * @param leaves      the element's leaves, by name relative to {@code path}, in schema order
 * @param unsupported why the collection cannot be read as a child relation, or {@code null} when it can. Set when an
 *                    element holds a collection with more than one leaf, whose values would arrive unpaired.
 */
public record CollectionSchema(String path, List<Leaf> leaves, @Nullable String unsupported) {

    /**
     * The prefix of the plan column that carries a collection's elements to a collection operator. The carrier cannot take
     * the collection's own name, which the dataset's column already holds, and a plan's columns are unique by name.
     */
    public static final String CARRIER_PREFIX = "$$collection.";

    public CollectionSchema {
        leaves = List.copyOf(leaves);
    }

    /** The name of the plan column that carries the collection at {@code path}. */
    public static String carrierName(String path) {
        return CARRIER_PREFIX + path;
    }

    /** Whether {@code column}, a scan's projected column, is the carrier of a collection rather than a column of the file. */
    public static boolean isCarrier(String column) {
        return column.startsWith(CARRIER_PREFIX);
    }

    /** The path of the collection the carrier column {@code column} carries. */
    public static String carriedPath(String column) {
        assert isCarrier(column) : column;
        return column.substring(CARRIER_PREFIX.length());
    }

    /**
     * One column of the child relation.
     *
     * @param name relative to the collection's path
     * @param type the column's type
     */
    public record Leaf(String name, DataType type) {}
}
