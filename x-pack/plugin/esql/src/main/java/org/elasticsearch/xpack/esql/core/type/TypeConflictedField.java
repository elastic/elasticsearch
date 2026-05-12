/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public sealed abstract class TypeConflictedField extends EsField permits InvalidMappedField, CompactInvalidMappedField {
    public TypeConflictedField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, esDataType, properties, aggregatable, timeSeriesFieldType);
    }

    public TypeConflictedField(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Pre-rendered, user-facing error message describing the conflict. Built from the full input map at construction time so it
     * survives the index-list truncation done by {@link CompactInvalidMappedField}.
     */
    public abstract String errorMessage();

    /**
     * Per-source-type indices in which the field appears with that type. Note that {@link CompactInvalidMappedField} caps each set
     * and may include the {@code "..."} sentinel; callers that need a complete index list should use {@link InvalidMappedField}
     * instead.
     */
    public abstract Map<String, Set<String>> getTypesToIndices();

    /** Whether the field is unmapped in at least one index, in which case it's treated as {@link DataType#KEYWORD} where it is unmapped. */
    public abstract boolean isPotentiallyUnmapped();

    /** Source data types observed for this field across all indices. */
    public abstract Set<DataType> types();

    // Utility functions used by implementors.

    static String makeErrorMessage(Map<String, Set<String>> typesToIndices, boolean includeInsistKeyword) {
        StringBuilder errorMessage = new StringBuilder();
        var isInsistKeywordOnlyKeyword = includeInsistKeyword && typesToIndices.containsKey(DataType.KEYWORD.typeName()) == false;
        errorMessage.append("mapped as [");
        errorMessage.append(typesToIndices.size() + (isInsistKeywordOnlyKeyword ? 1 : 0));
        errorMessage.append("] incompatible types: ");
        boolean first = true;
        if (isInsistKeywordOnlyKeyword) {
            first = false;
            errorMessage.append("[keyword] due to loading from _source");
        }
        for (Map.Entry<String, Set<String>> e : typesToIndices.entrySet()) {
            if (first) {
                first = false;
            } else {
                errorMessage.append(", ");
            }
            errorMessage.append("[");
            errorMessage.append(e.getKey());
            errorMessage.append("] ");
            if (e.getKey().equals(DataType.KEYWORD.typeName()) && includeInsistKeyword) {
                errorMessage.append("due to loading from _source and in ");
            } else {
                errorMessage.append("in ");
            }
            if (e.getValue().size() <= 3) {
                errorMessage.append(e.getValue());
            } else {
                errorMessage.append(e.getValue().stream().sorted().limit(3).collect(Collectors.toList()));
                errorMessage.append(" and [" + (e.getValue().size() - 3) + "] other ");
                errorMessage.append(e.getValue().size() == 4 ? "index" : "indices");
            }
        }
        return errorMessage.toString();
    }
}
