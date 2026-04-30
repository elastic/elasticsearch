/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Common surface for the two ESQL field flavors that carry per-index type-conflict information: the legacy
 * {@link InvalidMappedField} (full per-type index lists) and the memory-frugal {@link CompactInvalidMappedField} (truncated lists).
 * Production code that consumes these fields (analyzer rules, the verifier, type resolution) should branch on this interface so it
 * stays oblivious to which flavor a particular {@link EsField} happens to be.
 *
 * <p>The {@code getName} / {@code getProperties} / {@code isAggregatable} / {@code getTimeSeriesFieldType} accessors are all
 * provided for free by {@link EsField}, which both implementations extend; they're declared here so consumers can pull everything
 * they need off a single typed reference.
 */
public interface TypeConflictField {

    String getName();

    Map<String, EsField> getProperties();

    boolean isAggregatable();

    EsField.TimeSeriesFieldType getTimeSeriesFieldType();

    /**
     * Pre-rendered, user-facing error message describing the conflict. Built from the full input map at construction time so it
     * survives the index-list truncation done by {@link CompactInvalidMappedField}.
     */
    String errorMessage();

    /**
     * Per-source-type indices in which the field appears with that type. Note that {@link CompactInvalidMappedField} caps each set
     * and may include the {@code "..."} sentinel; callers that need a complete index list should use {@link InvalidMappedField}
     * instead.
     */
    Map<String, Set<String>> getTypesToIndices();

    /**
     * Whether the field is unmapped in at least one index, in which case it is treated as {@link DataType#KEYWORD} for the unmapped
     * indices.
     */
    boolean isPotentiallyUnmapped();

    /**
     * Source data types observed for this field across all indices.
     */
    default Set<DataType> types() {
        return getTypesToIndices().keySet().stream().map(DataType::fromTypeName).collect(Collectors.toSet());
    }

    /**
     * Build the user-facing error message for a per-type-to-indices map. Shared between both implementations so they stay in sync.
     */
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
