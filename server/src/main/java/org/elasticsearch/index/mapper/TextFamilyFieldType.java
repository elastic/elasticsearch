/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Map;

/**
 * This is a quality of life class that adds synthetic source context for text fields that need it.
 */
public abstract class TextFamilyFieldType extends StringFieldType {

    public static final String FALLBACK_FIELD_NAME_SUFFIX = "._original";
    private final boolean isSyntheticSourceEnabled;
    private final boolean isWithinMultiField;

    public TextFamilyFieldType(
        String name,
        IndexType indexType,
        boolean isStored,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta,
        boolean isSyntheticSourceEnabled,
        boolean isWithinMultiField
    ) {
        super(name, indexType, isStored, textSearchInfo, meta);
        this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
        this.isWithinMultiField = isWithinMultiField;
    }

    public boolean isSyntheticSourceEnabled() {
        return isSyntheticSourceEnabled;
    }

    public boolean isWithinMultiField() {
        return isWithinMultiField;
    }

    /**
     * Returns the name of the "fallback" field that can be used for synthetic source when the "main" field was not
     * stored for whatever reason.
     */
    public String syntheticSourceFallbackFieldName() {
        return name() + FALLBACK_FIELD_NAME_SUFFIX;
    }

    /**
     * Create an {@link IntervalsSource} for the given term.
     */
    public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create an {@link IntervalsSource} for the given prefix.
     */
    public IntervalsSource prefixIntervals(BytesRef prefix, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a fuzzy {@link IntervalsSource} for the given term.
     */
    public IntervalsSource fuzzyIntervals(
        String term,
        int maxDistance,
        int prefixLength,
        boolean transpositions,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a wildcard {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a regexp {@link IntervalsSource} for the given pattern.
     */
    public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * Create a range {@link IntervalsSource} for the given ranges
     */
    public IntervalsSource rangeIntervals(
        BytesRef lowerTerm,
        BytesRef upperTerm,
        boolean includeLower,
        boolean includeUpper,
        SearchExecutionContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
        );
    }

}
