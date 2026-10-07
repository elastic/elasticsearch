/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * Whether ES|QL treats a field as present on a shard. Field caps does not report dynamically-resolved
 * {@code flattened} sub-keys, and {@link org.elasticsearch.xpack.esql.session.IndexResolver} applies
 * {@code -nested} on the field-caps request. Yet both still resolve to a field type on the shard, and
 * {@code include_in_root} copies nested values onto the parent document. Value loading, planner stats and
 * pushed-down queries must all treat such fields as missing, or they disagree about the same rows.
 */
public final class ExtractableFields {
    private ExtractableFields() {}

    public static boolean isExtractable(SearchExecutionContext context, String field) {
        return isExtractable(field, context.isMappedField(field), context.nestedLookup());
    }

    /**
     * @param isMappedField whether the shard's mapping contains {@code field}, excluding dynamic {@code flattened}
     *                      sub-keys (see {@link org.elasticsearch.index.query.QueryRewriteContext#isMappedField})
     */
    public static boolean isExtractable(String field, boolean isMappedField, NestedLookup nestedLookup) {
        return isMappedField && nestedLookup.hasNestedParent(field) == false;
    }
}
