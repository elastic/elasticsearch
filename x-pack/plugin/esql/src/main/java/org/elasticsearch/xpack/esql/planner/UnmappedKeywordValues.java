/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Map;

/**
 * Projects a {@code _source} value onto the keyword type ES|QL fabricates for an unmapped field: an object (a {@code Map}) has no
 * keyword representation and contributes nothing, an array contributes its scalar elements (nested arrays flattened), and a scalar
 * becomes a single value. Shared by the per-document {@link UnmappedKeywordBlockLoader} and the coordinator-side
 * {@code ExpandUnmappedFieldsPostProcessor} so an explicitly referenced unmapped field and its {@code LOAD_ALL} auto-expanded twin
 * render identically.
 */
public final class UnmappedKeywordValues {

    private UnmappedKeywordValues() {}

    /**
     * Appends the keyword projection of {@code value} to {@code out}: an object (a {@code Map}) contributes nothing, an array
     * contributes the projection of each element (so nested arrays flatten and objects inside an array drop out), and any other value
     * contributes its {@code toString()} as one element. {@code out} is left untouched for a value with no keyword representation.
     */
    public static void collect(Object value, List<BytesRef> out) {
        if (value == null || value instanceof Map) {
            return;
        }
        if (value instanceof List<?> list) {
            for (Object element : list) {
                collect(element, out);
            }
            return;
        }
        out.add(new BytesRef(value.toString()));
    }
}
