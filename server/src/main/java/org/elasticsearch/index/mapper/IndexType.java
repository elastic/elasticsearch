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
 * What type of index structure is available for this field
 */
public class IndexType {

    /**
     * An IndexType with no index structures or doc values
     */
    public static final IndexType NONE = new IndexType(false, false, false, false, false, false);

    private final boolean hasTerms;
    private final boolean hasPoints;
    private final boolean hasPointsMetadata;
    private final boolean hasVectors;
    private final boolean hasDocValues;
    private final boolean hasDocValuesSkipper;

    private IndexType(
        boolean hasTerms,
        boolean hasPoints,
        boolean hasPointsMetadata,
        boolean hasVectors,
        boolean hasDocValues,
        boolean hasDocValuesSkipper
    ) {
        this.hasTerms = hasTerms;
        this.hasPoints = hasPoints;
        this.hasPointsMetadata = hasPointsMetadata;
        this.hasVectors = hasVectors;
        this.hasDocValues = hasDocValues;
        this.hasDocValuesSkipper = hasDocValuesSkipper;
    }

    /**
     * @return {@code true} if this IndexType has a Points index
     */
    public boolean hasPoints() {
        return hasPoints;
    }

    /**
     * @return {@code true} if this IndexType has Points metadata
     */
    public boolean hasPointsMetadata() {
        return hasPointsMetadata;
    }

    /**
     * @return {@code true} if this IndexType has an inverted index
     */
    public boolean hasTerms() {
        return hasTerms;
    }

    /**
     * @return {@code true} if this IndexType has a vector index
     */
    public boolean hasVectors() {
        return hasVectors;
    }

    /**
     * @return {@code true} if this IndexType has doc values
     */
    public boolean hasDocValues() {
        return hasDocValues;
    }

    /**
     * @return {@code true} if this IndexType has a doc values skipper
     */
    public boolean hasDocValuesSkipper() {
        return hasDocValuesSkipper;
    }

    /**
     * @return {@code true} if this IndexType has doc values but no index
     */
    public boolean hasOnlyDocValues() {
        return hasDocValues && hasDenseIndex() == false;
    }

    /**
     * @return {@code true} if this IndexType has a dense index structure
     */
    public boolean hasDenseIndex() {
        return hasPoints || hasTerms || hasVectors;
    }

    /**
     * @return {@code true} if this IndexType has index structures that support sort-based early termination
     */
    public boolean supportsSortShortcuts() {
        return hasTerms || hasPoints;
    }

    /**
     * @return an inverted-index based IndexType
     */
    public static IndexType terms(boolean isIndexed, boolean hasDocValues) {
        if (isIndexed && hasDocValues) {
            return new IndexType(true, false, false, false, true, false);
        }
        if (isIndexed) {
            return new IndexType(true, false, false, false, false, false);
        }
        if (hasDocValues) {
            return new IndexType(false, false, false, false, true, false);
        }
        return NONE;
    }

    /**
     * @return an IndexType with docValuesSkippers
     */
    public static IndexType skippers() {
        return new IndexType(false, false, false, false, true, true);
    }

    /**
     * @return a point-based IndexType
     */
    public static IndexType points(boolean isIndexed, boolean hasDocValues) {
        if (isIndexed && hasDocValues) {
            return new IndexType(false, true, true, false, true, false);
        }
        if (isIndexed) {
            return new IndexType(false, true, true, false, false, false);
        }
        if (hasDocValues) {
            return new IndexType(false, false, false, false, true, false);
        }
        return NONE;
    }

    /**
     * @return an IndexType representing archive data, with points metadata extracted from doc values
     */
    public static IndexType archivedPoints() {
        return new IndexType(false, false, true, false, true, false);
    }

    /**
     * @return an IndexType with doc values but no index
     */
    public static IndexType docValuesOnly() {
        return new IndexType(false, false, false, false, true, false);
    }

    /**
     * @return an IndexType with a vector index
     */
    public static IndexType vectors() {
        return new IndexType(false, false, false, true, false, false);
    }
}
