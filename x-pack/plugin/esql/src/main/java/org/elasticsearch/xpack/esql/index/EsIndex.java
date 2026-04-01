/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record EsIndex(
    String name,
    Map<String, EsField> mapping, // keyed by field names
    Map<String, IndexMode> indexNameWithModes,
    Map<String, List<String>> originalIndices, // keyed by cluster alias
    Map<String, List<String>> concreteIndices, // keyed by cluster alias
    Map<String, Set<String>> fieldToUnmappedIndices // keyed by field name; Set<String> are concrete index names.
) {

    public EsIndex {
        assert name != null;
        assert mapping != null;
        assert fieldToUnmappedIndices != null;
        assert fieldToUnmappedIndices.values().stream().noneMatch(Set::isEmpty);
    }

    public boolean isPartiallyUnmappedField(String fieldName) {
        return fieldToUnmappedIndices.containsKey(fieldName);
    }

    public Set<String> getUnmappedIndices(String fieldName) {
        return fieldToUnmappedIndices.getOrDefault(fieldName, Collections.emptySet());
    }

    public Set<String> concreteQualifiedIndices() {
        return indexNameWithModes.keySet();
    }

    @Override
    public String toString() {
        return name;
    }
}
