/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Map;
import java.util.Objects;

public class EsIndex {

    private final String name;
    private final Map<String, EsField> mapping;

    public EsIndex(String name, Map<String, EsField> mapping) {
        assert name != null;
        assert mapping != null;
        this.name = name;
        this.mapping = mapping;
    }

    public String name() {
        return name;
    }

    public Map<String, EsField> mapping() {
        return mapping;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mapping);
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
        return Objects.equals(name, other.name) && mapping == other.mapping;
    }
}
