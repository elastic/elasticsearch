/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class EsIndex {

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
