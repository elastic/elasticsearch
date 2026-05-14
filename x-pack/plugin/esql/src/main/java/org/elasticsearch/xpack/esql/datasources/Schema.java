/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Canonical representation of a schema in the datasources subsystem: a positional list of
 * attributes paired with a name set for fast membership checks. Used across the four-schema
 * model (see {@link SchemaReconciliation}). Immutable and thread-safe; the name set is built
 * eagerly at construction. Wire format is identical to a bare {@code List<Attribute>}.
 */
public final class Schema implements Writeable, Iterable<Attribute> {

    public static final Schema EMPTY = new Schema(List.of());

    private final List<Attribute> attributes;
    private final Set<String> names;

    public Schema(List<Attribute> attributes) {
        this.attributes = List.copyOf(attributes);
        this.names = this.attributes.stream().map(Attribute::name).collect(Collectors.toUnmodifiableSet());
    }

    public Schema(StreamInput in) throws IOException {
        this(in.readNamedWriteableCollectionAsList(Attribute.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(attributes);
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
        Schema schema = (Schema) o;
        return attributes.equals(schema.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }

    @Override
    public String toString() {
        return "Schema" + attributes;
    }
}
