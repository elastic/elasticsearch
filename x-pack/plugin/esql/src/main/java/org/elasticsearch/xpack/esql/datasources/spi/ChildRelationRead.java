/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * One child relation a read decodes beside its rows: a collection of the file (a list of structures), and the element
 * leaves to read from it. The reader reads only these leaves, in the same pass as the row's columns, and hands their
 * values back through {@link ChildRelationPages}. Naming the leaves rather than the collection keeps a wide element
 * from costing the read the leaves no consumer looks at.
 *
 * @param path   the collection's dotted name in the file's schema
 * @param leaves the leaves to read, by name relative to {@code path}, in the order {@link ChildWindows} emits them
 */
public record ChildRelationRead(String path, List<Leaf> leaves) {

    public ChildRelationRead {
        leaves = List.copyOf(leaves);
    }

    /**
     * One leaf of an element.
     *
     * @param name relative to the collection's path; a structure inside the element is dotted
     * @param type the type to emit the leaf as. A file that stores another type converts when
     *             {@link DeclaredTypeCoercions#supports} allows it, and fails the read otherwise
     */
    public record Leaf(String name, DataType type) {}
}
