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
 * and no element ever leaves the reader on its own.
 * <p>
 * <b>Output.</b> Each page holds the projected columns, in {@code projectedColumns} order, followed by one block per
 * {@link #leaves() leaf}, in request order. A file row with {@code n > 0} elements becomes {@code n} rows with its
 * column values repeated; a file row whose list is empty or null, or holds only null elements, becomes exactly one row
 * whose leaf columns are null. That is the index side's {@code NestedExpandOperator}, where a parent without nested
 * objects keeps one null row. A null element is dropped and announced, as a list column's null elements are.
 * <p>
 * <b>What counts as a row.</b> Everything the reader does per row is done on the file's rows, the parents, before they
 * are expanded: pushed filters and late materialization, {@code skip_row} drops, the read's row limit (which counts
 * parents), and the reader's own counters. Everything above the reader sees element rows. Consequently a read that
 * expands cannot report file statistics or row counts (see {@link FormatReadContext}), and cannot carry row positions
 * for deferred extraction, which address file rows: a read description that asks for both is refused. A layer above
 * the reader that drops a row for a parent column's value sees that value on every row of the parent, so it drops all
 * of them, which is dropping the parent; but it counts one error per row it drops, not one per parent.
 * <p>
 * The leaves are named rather than the element as a whole, so a wide element costs the read only what is asked of it.
 *
 * @param path   the collection's dotted name in the file's schema, after any rename to the physical name
 * @param leaves the leaves to emit, by name relative to {@code path}, in output order
 */
public record ExpandRead(String path, List<Leaf> leaves) {

    public ExpandRead {
        leaves = List.copyOf(leaves);
        Set<String> names = new HashSet<>();
        for (Leaf leaf : leaves) {
            if (names.add(leaf.name()) == false) {
                throw new IllegalArgumentException("leaf [" + leaf.name() + "] of [" + path + "] is requested twice");
            }
        }
    }

    /** This read with the collection renamed, for the logical-to-physical translation of a read's names. */
    public ExpandRead withPath(String physicalPath) {
        return physicalPath.equals(path) ? this : new ExpandRead(physicalPath, leaves);
    }

    /**
     * One leaf of an element.
     *
     * @param name relative to the collection's path; a structure inside the element is dotted
     * @param type the type to emit the leaf as. A file that stores another type converts when
     *             {@link DeclaredTypeCoercions#supports} allows it; when it does not, the read's error policy decides,
     *             as for a declared column whose type a file cannot be read as
     */
    public record Leaf(String name, DataType type) {}
}
