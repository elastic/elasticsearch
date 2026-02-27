/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Validates sort field compatibility across shard results and rewrites mixed INT/LONG sorts.
 */
public final class SortFieldValidation {

    private SortFieldValidation() {}

    public static Sort validateAndMaybeRewrite(Collection<? extends TopDocs> results, SortField[] firstSortFields) {
        Sort sort = new Sort(firstSortFields);
        if (results.size() < 2) {
            return sort;
        }

        SortField.Type[] firstTypes = null;
        boolean isFirstResult = true;
        Set<Integer> fieldIdsWithMixedIntAndLongSorts = new HashSet<>();
        Set<Integer> fieldIdsWithMixedNumericSorts = new HashSet<>();
        // Track which fields have floating point types to determine if we need DOUBLE conversion
        Set<Integer> fieldIdsWithFloatingPointTypes = new HashSet<>();
        for (TopDocs topDocs : results) {
            // We don't actually merge in empty score docs, so ignore potentially mismatched types if there are no docs
            if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0) {
                continue;
            }
            SortField[] curSortFields = ((TopFieldDocs) topDocs).fields;
            if (isFirstResult) {
                sort = new Sort(curSortFields);
                firstTypes = new SortField.Type[curSortFields.length];
                for (int i = 0; i < curSortFields.length; i++) {
                    firstTypes[i] = getType(curSortFields[i]);
                    if (firstTypes[i] == SortField.Type.CUSTOM) {
                        // for custom types that we can't resolve, we can't do the check
                        return sort;
                    }
                    // Track if this field has floating point types
                    if (firstTypes[i] == SortField.Type.FLOAT || firstTypes[i] == SortField.Type.DOUBLE) {
                        fieldIdsWithFloatingPointTypes.add(i);
                    }
                }
                isFirstResult = false;
            } else {
                for (int i = 0; i < curSortFields.length; i++) {
                    SortField.Type curType = getType(curSortFields[i]);
                    if (curType != firstTypes[i]) {
                        if (curType == SortField.Type.CUSTOM) {
                            // for custom types that we can't resolve, we can't do the check
                            return sort;
                        }
                        // Track if this field has floating point types
                        if (curType == SortField.Type.FLOAT || curType == SortField.Type.DOUBLE) {
                            fieldIdsWithFloatingPointTypes.add(i);
                        }
                        // Check if we are mixing INT and LONG sort types (without floating point)
                        if (mixIntAndLong(firstTypes[i], curType)) {
                            // Only add to INT/LONG mixing if there's no floating point type for this field
                            if (fieldIdsWithFloatingPointTypes.contains(i) == false) {
                                fieldIdsWithMixedIntAndLongSorts.add(i);
                            } else {
                                // If floating point exists, convert everything to DOUBLE
                                fieldIdsWithMixedNumericSorts.add(i);
                            }
                        } else if (isNumericType(firstTypes[i])
                            && isNumericType(curType)
                            && involvesFloatingPoint(firstTypes[i], curType)) {
                                // Only convert to DOUBLE when the mix involves FLOAT or DOUBLE (never pure INT/LONG).
                                fieldIdsWithMixedNumericSorts.add(i);
                            } else {
                                throw new IllegalArgumentException(
                                    "Can't sort on field ["
                                        + curSortFields[i].getField()
                                        + "]; the field has incompatible sort types: ["
                                        + firstTypes[i]
                                        + "] and ["
                                        + curType
                                        + "] across shards!"
                                );
                            }
                    }
                }
            }
        }
        // Remove fields from INT/LONG mixing if they also need DOUBLE conversion
        fieldIdsWithMixedIntAndLongSorts.removeAll(fieldIdsWithMixedNumericSorts);
        if (fieldIdsWithMixedIntAndLongSorts.isEmpty() == false) {
            // Ensure INT/LONG-only fields are rewritten to LONG, never to DOUBLE
            for (int fieldIdx : fieldIdsWithMixedIntAndLongSorts) {
                SortField.Type type = firstTypes[fieldIdx];
                assert type == SortField.Type.INT || type == SortField.Type.LONG
                    : "INT/LONG mix must be rewritten to LONG, not DOUBLE; field " + fieldIdx + " had type " + type;
            }
            sort = rewriteSortAndResultsToLong(sort, results, fieldIdsWithMixedIntAndLongSorts);
        }
        if (fieldIdsWithMixedNumericSorts.isEmpty() == false) {
            sort = rewriteSortAndResultsToDouble(sort, results, fieldIdsWithMixedNumericSorts);
        }
        return sort;
    }

    private static boolean mixIntAndLong(SortField.Type firstType, SortField.Type currentType) {
        return (firstType == SortField.Type.INT && currentType == SortField.Type.LONG)
            || (firstType == SortField.Type.LONG && currentType == SortField.Type.INT);
    }

    /**
     * True when at least one type is FLOAT or DOUBLE. Used to ensure we only add to the
     * DOUBLE-rewrite set when the mix involves floating point, never for pure INT/LONG.
     */
    private static boolean involvesFloatingPoint(SortField.Type a, SortField.Type b) {
        return (a == SortField.Type.FLOAT || a == SortField.Type.DOUBLE) || (b == SortField.Type.FLOAT || b == SortField.Type.DOUBLE);
    }

    /**
     * Rewrite Sort objects and shards results for long sort for mixed fields:
     * convert Sort to Long sort and convert fields' values to Long values.
     * This is necessary to enable comparison of fields' values across shards for merging.
     */
    private static Sort rewriteSortAndResultsToLong(
        Sort sort,
        Collection<? extends TopDocs> results,
        Set<Integer> fieldIdsWithMixedIntAndLongSorts
    ) {
        SortField[] newSortFields = sort.getSort();
        for (int fieldIdx : fieldIdsWithMixedIntAndLongSorts) {
            for (TopDocs topDocs : results) {
                if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0) {
                    continue;
                }
                SortField[] sortFields = ((TopFieldDocs) topDocs).fields;
                if (getType(sortFields[fieldIdx]) == SortField.Type.INT) {
                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                        fieldDoc.fields[fieldIdx] = ((Number) fieldDoc.fields[fieldIdx]).longValue();
                    }
                } else { // SortField.Type.LONG
                    newSortFields[fieldIdx] = sortFields[fieldIdx];
                }
            }
        }
        return new Sort(newSortFields);
    }

    /**
     * Rewrite Sort objects and shard results for double sort for mixed numeric fields:
     * convert Sort to Double sort and convert fields' values to Double values.
     * This handles FLOAT/LONG/DOUBLE mixing by converting all to DOUBLE.
     */
    private static Sort rewriteSortAndResultsToDouble(
        Sort sort,
        Collection<? extends TopDocs> results,
        Set<Integer> fieldIdsWithMixedNumericSorts
    ) {
        SortField[] newSortFields = sort.getSort();
        for (int fieldIdx : fieldIdsWithMixedNumericSorts) {
            // Rewrite the sort field to DOUBLE
            SortField originalField = newSortFields[fieldIdx];
            SortField doubleField = new SortField(originalField.getField(), SortField.Type.DOUBLE, originalField.getReverse());
            doubleField.setMissingValue(originalField.getMissingValue());
            newSortFields[fieldIdx] = doubleField;

            // Convert all sort values to Double
            for (TopDocs topDocs : results) {
                if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0) {
                    continue;
                }
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    Object value = fieldDoc.fields[fieldIdx];
                    if (value != null && value instanceof Number) {
                        fieldDoc.fields[fieldIdx] = ((Number) value).doubleValue();
                    }
                }
            }
        }
        return new Sort(newSortFields);
    }

    private static boolean isNumericType(SortField.Type type) {
        return type == SortField.Type.INT || type == SortField.Type.LONG || type == SortField.Type.FLOAT || type == SortField.Type.DOUBLE;
    }

    private static SortField.Type getType(SortField sortField) {
        if (sortField instanceof SortedNumericSortField sf) {
            return sf.getNumericType();
        } else if (sortField instanceof SortedSetSortField) {
            return SortField.Type.STRING;
        } else if (sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource cmp) {
            // This can occur if the sort field wasn't rewritten by Lucene#rewriteMergeSortField because all search shards are local.
            return cmp.reducedType();
        } else {
            return sortField.getType();
        }
    }
}
