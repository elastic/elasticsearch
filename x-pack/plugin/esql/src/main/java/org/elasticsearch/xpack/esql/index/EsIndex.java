/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;
import java.util.Set;

public record EsIndex(
    String name,
    Map<String, EsField> mapping, // keyed by field names
    Map<String, IndexMode> indexNameWithModes,
    Set<String> partiallyUnmappedFields
) {

    public EsIndex {
        assert name != null;
        assert mapping != null;
        assert partiallyUnmappedFields != null;
    }

    public boolean isPartiallyUnmappedField(String fieldName) {
        return partiallyUnmappedFields.contains(fieldName);
    }

    public Set<String> concreteIndices() {
        return indexNameWithModes.keySet();
    }

    @Override
    public String toString() {
        return name;
    }
}
