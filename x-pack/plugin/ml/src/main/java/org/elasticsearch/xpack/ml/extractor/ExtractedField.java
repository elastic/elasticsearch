/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.Set;

/**
 * Describes how to extract an analyzed field
 */
public interface ExtractedField {

    enum Method {
        SOURCE,
        DOC_VALUE,
        SCRIPT_FIELD
    }

    /**
     * @return The name of the field as expected by the user
     */
    String getName();

    /**
     * This is the name of the field we should search for.
     * In most cases this is the same as {@link #getName()}.
     * However, if the field is a non-aggregatable multi-field
     * we cannot retrieve it from source. Thus we search for
     * its parent instead.
     * @return The name of the field that is searched.
     */
    String getSearchField();

    /**
     * @return The field types
     */
    Set<String> getTypes();

    /**
     * @return The extraction {@link Method}
     */
    Method getMethod();

    /**
     * Extracts the value from a {@link SearchHit}
     *
     * @param hit    the search hit
     * @param source the source supplier
     * @return the extracted value
     */
    Object[] value(SearchHit hit, SourceSupplier source);

    /**
     * @return Whether the field can be fetched from source instead
     */
    boolean supportsFromSource();

    /**
     * @return A new extraction field that's fetching from source
     */
    ExtractedField newFromSource();

    /**
     * @return Whether it is a multi-field
     */
    boolean isMultiField();

    /**
     * @return The multi-field parent
     */
    default String getParentField() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The doc_value format
     */
    default String getDocValueFormat() {
        throw new UnsupportedOperationException();
    }
}
