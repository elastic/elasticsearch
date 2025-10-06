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
public enum IndexType {

    /**
     * An inverted index of terms with doc values
     */
    TERMS,

    /**
     * An inverted index of terms without doc values
     */
    TERMS_WITHOUT_DOC_VALUES,

    /**
     * A BKD-tree with doc values
     */
    POINTS,

    /**
     * A BKD-tree without doc values
     */
    POINTS_WITHOUT_DOC_VALUES,

    /**
     * Archive indexes: BKD metadata reconstructed from doc values, but no actual BKD tree
     */
    POINTS_METADATA,

    /**
     * A sparse index over doc-values
     */
    SPARSE,

    /**
     * A knn vector tree
     */
    VECTOR,

    /**
     * Doc values but no index structures
     */
    DOC_VALUES_ONLY,

    /**
     * No index structures are stored for this field
     */
    NONE;

    /**
     * @return {@code true} if this IndexType has a Points index
     */
    public static boolean hasPoints(IndexType type) {
        return type == POINTS || type == POINTS_WITHOUT_DOC_VALUES;
    }

    /**
     * @return {@code true} if this IndexType has Points metadata
     */
    public static boolean hasPointsMetadata(IndexType type) {
        return type == POINTS_METADATA || hasPoints(type);
    }

    /**
     * @return {@code true} if this IndexType has an inverted index
     */
    public static boolean hasTerms(IndexType type) {
        return type == TERMS || type == TERMS_WITHOUT_DOC_VALUES;
    }

    /**
     * @return {@code true} if this IndexType has doc values
     */
    public static boolean hasDocValues(IndexType type) {
        return type == POINTS || type == POINTS_METADATA || type == TERMS || type == SPARSE || type == DOC_VALUES_ONLY;
    }

    /**
     * @return {@code true} if this IndexType has any index structure at all
     */
    public static boolean isIndexed(IndexType type) {
        return hasPoints(type) || hasTerms(type) || type == VECTOR;
    }

    /**
     * @return {@code true} if this IndexType has index structures that support sort-based early termination
     */
    public static boolean supportsSortShortcuts(IndexType type) {
        return hasTerms(type) || hasPoints(type);
    }

    /**
     * @return an inverted-index based IndexType
     */
    public static IndexType terms(boolean isIndexed, boolean hasDocValues) {
        if (isIndexed && hasDocValues) {
            return IndexType.TERMS;
        }
        if (isIndexed) {
            return IndexType.TERMS_WITHOUT_DOC_VALUES;
        }
        return hasDocValues ? IndexType.DOC_VALUES_ONLY : IndexType.NONE;
    }

    /**
     * @return a point-based IndexType
     */
    public static IndexType points(boolean isIndexed, boolean hasDocValues, boolean archive) {
        if (isIndexed && hasDocValues) {
            return archive ? IndexType.POINTS_METADATA : IndexType.POINTS;
        }
        if (isIndexed) {
            return IndexType.POINTS_WITHOUT_DOC_VALUES;
        }
        return hasDocValues ? IndexType.DOC_VALUES_ONLY : IndexType.NONE;
    }
}
