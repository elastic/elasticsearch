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
    // The roots whose whole _source subtree LOAD_ALL must not expand into columns: paths some concrete index maps as nested or
    // flattened. Both are kept out of unmapped expansion - nested is unsupported, and a flattened field's dotted keys are its own,
    // not unmapped leaves. Only resolved for LOAD_ALL.
    Set<String> sourceExcludedSubtrees
) {

    public EsIndex {
        assert name != null;
        assert mapping != null;
        assert sourceExcludedSubtrees != null;
    }

    public EsIndex(
        String name,
        Map<String, EsField> mapping,
        Map<String, IndexProperties> indexProperties,
        Map<String, List<String>> originalIndices,
        Map<String, List<String>> concreteIndices
    ) {
        this(name, mapping, indexProperties, originalIndices, concreteIndices, Set.of());
    }

    public EsIndex withSourceExcludedSubtrees(Set<String> sourceExcludedSubtrees) {
        return new EsIndex(name, mapping, indexProperties, originalIndices, concreteIndices, Set.copyOf(sourceExcludedSubtrees));
    }

    public Set<String> concreteQualifiedIndices() {
        return indexProperties.keySet();
    }

    @Override
    public String toString() {
        return name;
    }
}
