/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Coordinator-only carrier for the four-schema-model units (File, Unified, Query, Per-file Query —
 * see {@link SchemaReconciliation}) inside the datasources subsystem. Pairs a positional
 * {@code List<Attribute>} with a name set built eagerly at construction for O(1) membership tests
 * at the prune / skip / map-filter seams. Not a wire form — schemas that travel across nodes go
 * through their own paths (post-prune {@code attributes} on {@code ExternalSourceExec} via the
 * planner contract, and {@code FileSplit.readSchema} via primitive {@code (name, type, nullable)}
 * tuples).
 */
public final class ExternalSchema implements Iterable<Attribute> {

    public static final ExternalSchema EMPTY = new ExternalSchema(List.of());

    /**
     * Returns the data-attribute view of {@code attributes} as an {@link ExternalSchema}: the
     * input list with virtual columns filtered out (relative order preserved). Used by
     * external-source operator factories on the data node to derive the data-only schema once at
     * construction rather than re-slicing per page; virtual columns ({@link MetadataAttribute}
     * for ES document metadata, any {@link VirtualAttribute} for engine-synthesized columns like
     * {@code _file.*}) are appended on the producer thread by {@code VirtualColumnIterator} and
     * must not appear in the data-channel layout the format reader sees. Hive-style partition
     * columns are real {@link org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute}s
     * here and pass through; their projection stripping happens at the projection seam by name.
     */
    public static ExternalSchema dataAttributesOf(List<Attribute> attributes) {
        List<Attribute> data = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            if (PushdownPredicates.isVirtualColumn(attr) == false) {
                data.add(attr);
            }
        }
        return new ExternalSchema(data);
    }

    private final List<Attribute> attributes;
    private final Set<String> names;

    public ExternalSchema(List<Attribute> attributes) {
        this.attributes = List.copyOf(attributes);
        this.names = this.attributes.stream().map(Attribute::name).collect(Collectors.toUnmodifiableSet());
    }

    public List<Attribute> attributes() {
        return attributes;
    }

    public Set<String> names() {
        return names;
    }

    public int size() {
        return attributes.size();
    }

    public boolean isEmpty() {
        return attributes.isEmpty();
    }

    public Attribute get(int index) {
        return attributes.get(index);
    }

    @Override
    public Iterator<Attribute> iterator() {
        return attributes.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExternalSchema schema = (ExternalSchema) o;
        return attributes.equals(schema.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }

    @Override
    public String toString() {
        return "ExternalSchema" + attributes;
    }
}
