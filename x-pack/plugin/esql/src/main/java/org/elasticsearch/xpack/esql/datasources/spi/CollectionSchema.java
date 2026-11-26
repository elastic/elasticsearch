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
 * columns are the element's leaves. A list of structures and a map are the same thing here; a map's element is its
 * entry, whose leaves are {@code key} and the leaves of {@code value}.
 *
 * @param path        the collection's dotted name in the file's schema, as the query names it
 * @param keyLeaf     the leaf the format designates as the element's key, or {@code null} when none is, as for a list
 * @param leaves      the element's leaves, by name relative to {@code path}, in schema order
 * @param unsupported why the collection cannot be read as a child relation, or {@code null} when it can. Set when an
 *                    element holds a collection with more than one leaf, whose values would arrive unpaired.
 */
public record CollectionSchema(String path, @Nullable String keyLeaf, List<Leaf> leaves, @Nullable String unsupported) {

    public CollectionSchema {
        leaves = List.copyOf(leaves);
    }

    /**
     * One column of the child relation.
     *
     * @param name relative to the collection's path
     * @param type the column's type
     */
    public record Leaf(String name, DataType type) {}
}
