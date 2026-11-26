/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tells a format reader to read one collection of the file (a list of structures) at element grain: instead of one row
 * per file row, the read emits one row per element, so a dataset's {@code NESTED_EXPAND} costs one pass over the file
 * and no element ever leaves the reader on its own. Only a reader whose {@link FormatReader#supportsExpandRead()} says
 * so may be handed one.
 * <p>
 * <b>Output.</b> Each page holds the projected columns, in {@code projectedColumns} order, followed by one block per
 * {@link #leaves() leaf}, in request order. A file row with {@code n > 0} elements becomes {@code n} rows with its
 * column values repeated; a file row whose list is empty or null, or holds only null elements, becomes exactly one row
 * whose leaf columns are null. That is the index side's {@code NestedExpandOperator}, where a parent without nested
 * objects keeps one null row. A null element is dropped and announced, as a list column's null elements are.
 * <p>
 * <b>What counts as a row.</b> Everything the reader does per row is done on the file's rows, the parents, before they
 * are expanded: pushed filters and late materialization, {@code skip_row} drops (an element value that cannot be read
 * at its leaf's type drops its parent, as a column value does its row), the read's row limit (which counts parents), and
 * the reader's own counters. Everything above the reader sees element rows. Consequently a read that expands cannot
 * report file statistics or row counts (see {@link FormatReadContext}), and cannot carry row positions for deferred
 * extraction, which address file rows: a read description that asks for both is refused.
 * <p>
 * <b>Parent ordinals.</b> With {@code parentOrdinals}, every page ends with one more block, after the leaves: an
 * {@code IntBlock} holding, per row, the ordinal within the page of the file row it was expanded from. Ordinals start at
 * {@code 0} and step by one from one parent to the next, and a parent's rows never span two pages. A layer above the
 * reader that drops rows for a parent column's value drops every row of that parent; it reads the ordinals to charge its
 * error budget once per parent, as the reader does, rather than once per element. The caller removes the block before
 * the page leaves the source.
 * <p>
 * The leaves are named rather than the element as a whole, so a wide element costs the read only what is asked of it.
 * The list may be empty: the elements are then only counted, which a query that reads none of their fields still needs
 * to repeat each parent the right number of times.
 * <p>
 * The {@code path} is the collection's name in the files, as schema discovery recorded it from their metadata, and not
 * a column name of the dataset: a declared rename does not apply to it.
 *
 * @param path           the collection's dotted name in the file's schema
 * @param leaves         the leaves to emit, by name relative to {@code path}, in output order
 * @param parentOrdinals whether each page carries the trailing parent-ordinal block
 */
public record ExpandRead(String path, List<Leaf> leaves, boolean parentOrdinals) {

    public ExpandRead {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("an expanding read needs the path of the collection to expand");
        }
        leaves = List.copyOf(leaves);
        Set<String> names = new HashSet<>();
        for (Leaf leaf : leaves) {
            if (leaf.name() == null || leaf.name().isEmpty()) {
                throw new IllegalArgumentException("a leaf of [" + path + "] needs a name");
            }
            if (names.add(leaf.name()) == false) {
                throw new IllegalArgumentException("leaf [" + leaf.name() + "] of [" + path + "] is requested twice");
            }
        }
    }

    /** A read whose pages carry no parent ordinals. */
    public ExpandRead(String path, List<Leaf> leaves) {
        this(path, leaves, false);
    }

    /** The blocks each page carries after the projected columns: the leaves, then the parent ordinals if asked for. */
    public int trailingBlocks() {
        return leaves.size() + (parentOrdinals ? 1 : 0);
    }

    /**
     * One leaf of an element.
     *
     * @param name relative to the collection's path; a structure inside the element is dotted
     * @param type the type to emit the leaf as: the type schema discovery merged across the files, so each file stores
     *             it or a type that widens to it ({@link TypeWidening#join}). A file whose type does not widen to it
     *             reads the leaf as null, with a warning, as an inferred column whose type a file does not match does
     */
    public record Leaf(String name, DataType type) {}
}
