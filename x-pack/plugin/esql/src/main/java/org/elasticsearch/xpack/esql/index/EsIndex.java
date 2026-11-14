/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.List;
import java.util.Map;
import java.util.Set;

public record EsIndex(
    String name,
    Map<String, EsField> mapping, // keyed by field names
    Map<String, IndexProperties> indexProperties, // keyed by concrete index name
    Map<String, List<String>> originalIndices, // keyed by cluster alias
    Map<String, List<String>> concreteIndices, // keyed by cluster alias
    Map<String, FieldSemantics> fieldSemantics // keyed by full field name; coordinator only
) {

    public EsIndex {
        assert name != null;
        assert mapping != null;
        assert fieldSemantics != null;
    }

    public EsIndex(
        String name,
        Map<String, EsField> mapping,
        Map<String, IndexProperties> indexProperties,
        Map<String, List<String>> originalIndices,
        Map<String, List<String>> concreteIndices
    ) {
        this(name, mapping, indexProperties, originalIndices, concreteIndices, Map.of());
    }

    public Set<String> concreteQualifiedIndices() {
        return indexProperties.keySet();
    }

    @Override
    public String toString() {
        return name;
    }
}
