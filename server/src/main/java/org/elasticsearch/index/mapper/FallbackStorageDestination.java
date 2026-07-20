/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * The possible storage destinations for a document field value that cannot be indexed normally.
 * <p>
 * Every fallback write lands in exactly one destination; the choice is made by
 * {@link FallbackStorageRouter#route}.
 */
public enum FallbackStorageDestination {

    /**
     * The value is preserved in the {@code _ignored_source} metadata field and included verbatim
     * in synthetic {@code _source} reconstruction. Used for fields that cannot reconstruct their
     * value from doc values (synthetic source fallback mode), fields with {@code source_keep},
     * copy-to destinations, and unmapped fields under {@code dynamic: false} or
     * {@code dynamic: runtime}.
     */
    IGNORED_SOURCE,

    /**
     * The value is stored in a per-field {@code fieldPath._ignore_malformed} column (binary doc
     * values on new indices; stored field on old indices). Used when a value fails to parse with
     * {@code ignore_malformed: true}, so that synthetic {@code _source} reconstruction can still
     * reproduce the original value verbatim.
     */
    IGNORE_MALFORMED,

    /**
     * The value is stored in a per-field {@code fieldPath._on_failure} binary doc values column.
     * Used when a {@code multi_value: false} field receives a duplicate value and the field is
     * configured with {@code doc_values.on_failure: ignore}, so that indexing continues without
     * the excess value reaching the field's own doc values.
     */
    ON_FAILURE;
}
