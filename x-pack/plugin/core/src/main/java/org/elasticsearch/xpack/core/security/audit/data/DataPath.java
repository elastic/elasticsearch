/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import java.util.List;
import java.util.Optional;

/**
 * A structured query into a {@link DataValue} tree, expressed as an ordered list of {@link Element}s (object field names
 * and array indices). Resolving a path never requires an unchecked cast and yields an empty result rather than throwing
 * when the shape does not match.
 */
public record DataPath(List<Element> elements) {

    /**
     * A single step in a {@link DataPath}.
     */
    public sealed interface Element permits Field, Index {}

    /**
     * Selects a field of a {@link DataObject}.
     */
    public record Field(String name) implements Element {}

    /**
     * Selects an element of a {@link DataArray}.
     */
    public record Index(int value) implements Element {}

    /**
     * Resolves this path against {@code root}.
     *
     * @return the value at this path, or {@link Optional#empty()} if any step does not match the tree's shape or is out
     *         of bounds
     */
    public Optional<DataValue> query(DataValue root) {
        DataValue current = root;

        for (Element element : elements) {
            final DataValue node = current;
            switch (element) {
                case Field(String name) when node instanceof DataObject object -> {
                    Optional<DataValue> next = object.get(name);
                    if (next.isEmpty()) {
                        return Optional.empty();
                    }
                    current = next.get();
                }
                case Index(int index) when node instanceof DataArray array -> {
                    if (index < 0 || index >= array.size()) {
                        return Optional.empty();
                    }
                    current = array.get(index);
                }
                // the current value's type does not match the step (e.g. a field access on a non-object): no match
                default -> {
                    return Optional.empty();
                }
            }
        }

        return Optional.of(current);
    }
}
