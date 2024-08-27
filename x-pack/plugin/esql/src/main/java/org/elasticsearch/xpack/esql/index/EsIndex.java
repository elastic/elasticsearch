/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class EsIndex implements Writeable {

    private final String name;
    private final Map<String, EsField> mapping;
    private final Set<String> concreteIndices;

    public EsIndex(String name, Map<String, EsField> mapping) {
        this(name, mapping, Set.of());
    }

    public EsIndex(String name, Map<String, EsField> mapping, Set<String> concreteIndices) {
        assert name != null;
        assert mapping != null;
        this.name = name;
        this.mapping = mapping;
        this.concreteIndices = concreteIndices;
    }

    @SuppressWarnings("unchecked")
    public EsIndex(StreamInput in) throws IOException {
        this(in.readString(), in.readImmutableMap(StreamInput::readString, EsField::readFrom), (Set<String>) in.readGenericValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name());
        out.writeMap(mapping(), (o, x) -> x.writeTo(out));
        out.writeGenericValue(concreteIndices());
    }

    public String name() {
        return name;
    }

    public Map<String, EsField> mapping() {
        return mapping;
    }

    public Set<String> concreteIndices() {
        return concreteIndices;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mapping, concreteIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsIndex other = (EsIndex) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(mapping, other.mapping)
            && Objects.equals(concreteIndices, other.concreteIndices);
    }
}
