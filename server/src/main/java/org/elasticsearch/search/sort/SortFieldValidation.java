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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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

        // We don't actually merge in empty score docs, so ignore potentially mismatched types if there are no docs
        List<TopFieldDocs> nonEmptyResults = new ArrayList<>(results.size());
        for (TopDocs topDocs : results) {
            if (topDocs != null && topDocs.scoreDocs != null && topDocs.scoreDocs.length > 0) {
                nonEmptyResults.add((TopFieldDocs) topDocs);
            }
        }
        if (nonEmptyResults.isEmpty()) {
            return sort;
        }
        sort = new Sort(nonEmptyResults.getFirst().fields);

        int numFields = sort.getSort().length;
        // For each sort position, the type and sort field of the first shard that really has values for it (see unmappedPlaceholders)
        SortField.Type[] firstTypes = new SortField.Type[numFields];
        SortField[] firstSortFieldsWithValues = new SortField[numFields];
        boolean[][] unmappedPlaceholders = new boolean[nonEmptyResults.size()][];
        Set<Integer> fieldIdsWithMixedIntAndLongSorts = new HashSet<>();
        Set<Integer> fieldIdsWithMixedNumericSorts = new HashSet<>();
        // Track which fields have floating point types to determine if we need DOUBLE conversion
        Set<Integer> fieldIdsWithFloatingPointTypes = new HashSet<>();
        for (int shard = 0; shard < nonEmptyResults.size(); shard++) {
            TopFieldDocs topFieldDocs = nonEmptyResults.get(shard);
            SortField[] curSortFields = topFieldDocs.fields;
            unmappedPlaceholders[shard] = findUnmappedPlaceholders(topFieldDocs);
            for (int i = 0; i < curSortFields.length; i++) {
                if (unmappedPlaceholders[shard][i]) {
                    continue;
                }
                SortField.Type curType = getType(curSortFields[i]);
                if (curType == SortField.Type.CUSTOM) {
                    // for custom types that we can't resolve, we can't do the check
                    return new Sort(nonEmptyResults.getFirst().fields);
                }
                if (firstTypes[i] == null) {
                    firstTypes[i] = curType;
                    firstSortFieldsWithValues[i] = curSortFields[i];
                    // Track if this field has floating point types
                    if (curType == SortField.Type.FLOAT || curType == SortField.Type.DOUBLE) {
                        fieldIdsWithFloatingPointTypes.add(i);
                    }
                } else if (curType != firstTypes[i]) {
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
                    } else if (isNumericType(firstTypes[i]) && isNumericType(curType) && involvesFloatingPoint(firstTypes[i], curType)) {
                        // Only convert to DOUBLE when the mix involves FLOAT or DOUBLE (never pure INT/LONG).
                        fieldIdsWithMixedNumericSorts.add(i);
                    } else {
                        throw incompatibleSortTypes(curSortFields[i], firstTypes[i], curType);
                    }
                }
            }
        }

        // A placeholder can only be reconciled with a numeric sort (by giving its docs the numeric "missing" value),
        // or with a string sort (where null values are already handled by the comparator).
        for (int shard = 0; shard < nonEmptyResults.size(); shard++) {
            for (int i = 0; i < numFields; i++) {
                if (unmappedPlaceholders[shard][i]
                    && firstTypes[i] != null
                    && firstTypes[i] != SortField.Type.STRING
                    && isNumericType(firstTypes[i]) == false) {
                    throw incompatibleSortTypes(nonEmptyResults.get(shard).fields[i], firstTypes[i], SortField.Type.STRING);
                }
            }
        }

        // The merge must compare using the sort field of a shard that has values, not of a leading placeholder shard.
        SortField[] mergeSortFields = sort.getSort().clone();
        for (int i = 0; i < numFields; i++) {
            if (firstSortFieldsWithValues[i] != null) {
                mergeSortFields[i] = firstSortFieldsWithValues[i];
            }
        }
        sort = new Sort(mergeSortFields);

        // Remove fields from INT/LONG mixing if they also need DOUBLE conversion
        fieldIdsWithMixedIntAndLongSorts.removeAll(fieldIdsWithMixedNumericSorts);
        if (fieldIdsWithMixedIntAndLongSorts.isEmpty() == false) {
            // Ensure INT/LONG-only fields are rewritten to LONG, never to DOUBLE
            for (int fieldIdx : fieldIdsWithMixedIntAndLongSorts) {
                SortField.Type type = firstTypes[fieldIdx];
                assert type == SortField.Type.INT || type == SortField.Type.LONG
                    : "INT/LONG mix must be rewritten to LONG, not DOUBLE; field " + fieldIdx + " had type " + type;
            }
            sort = rewriteSortAndResultsToLong(sort, nonEmptyResults, unmappedPlaceholders, fieldIdsWithMixedIntAndLongSorts);
        }
        if (fieldIdsWithMixedNumericSorts.isEmpty() == false) {
            sort = rewriteSortAndResultsToDouble(sort, nonEmptyResults, fieldIdsWithMixedNumericSorts);
        }
        // Done last, so that the placeholders' values are built for the final (possibly widened) type of the field.
        for (int i = 0; i < numFields; i++) {
            if (firstTypes[i] != null && isNumericType(firstTypes[i])) {
                SortField.Type finalType = fieldIdsWithMixedNumericSorts.contains(i) ? SortField.Type.DOUBLE
                    : fieldIdsWithMixedIntAndLongSorts.contains(i) ? SortField.Type.LONG
                    : firstTypes[i];
                rewriteUnmappedPlaceholders(nonEmptyResults, unmappedPlaceholders, i, finalType);
            }
        }
        return sort;
    }

    private static IllegalArgumentException incompatibleSortTypes(SortField sortField, SortField.Type firstType, SortField.Type curType) {
        return new IllegalArgumentException(
            "Can't sort on field ["
                + sortField.getField()
                + "]; the field has incompatible sort types: ["
                + firstType
                + "] and ["
                + curType
                + "] across shards!"
        );
    }

    /**
     * Finds the sort positions for which a shard only holds the placeholder produced by {@code unmapped_type}: a STRING sort whose
     * every returned doc has a null sort value. When the field is mapped in some indices and not in others, the shards of the
     * latter have docs but none can have a value for the field, so the placeholder type says nothing about the type of the field
     * and must not conflict with the type of the shards where the field is mapped.
     * Shards where the missing value was customized hold actual values, so they are never considered placeholders.
     */
    private static boolean[] findUnmappedPlaceholders(TopFieldDocs topFieldDocs) {
        SortField[] sortFields = topFieldDocs.fields;
        boolean[] placeholders = new boolean[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            if (getType(sortFields[i]) == SortField.Type.STRING && isMissingFirstOrLast(sortFields[i])) {
                boolean allNull = true;
                for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
                    if (((FieldDoc) scoreDoc).fields[i] != null) {
                        allNull = false;
                        break;
                    }
                }
                placeholders[i] = allNull;
            }
        }
        return placeholders;
    }

    /**
     * Whether the STRING sort field puts docs without a value first or last, rather than filling them with a custom value.
     * {@link SortField#STRING_FIRST} and {@link SortField#STRING_LAST} already account for the sort direction.
     */
    private static boolean isMissingFirstOrLast(SortField sortField) {
        Object missingValue = stringMissingValue(sortField);
        return missingValue == SortField.STRING_FIRST || missingValue == SortField.STRING_LAST;
    }

    private static Object stringMissingValue(SortField sortField) {
        Object missingValue = sortField.getMissingValue();
        if (missingValue == null && sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource cmp) {
            // This can occur if the sort field wasn't rewritten by Lucene#rewriteMergeSortField because all search shards are local.
            missingValue = cmp.missingValue(sortField.getReverse());
        }
        return missingValue;
    }

    /**
     * Whether a {@code search_after} value is the numeric missing value that {@link #validateAndMaybeRewrite} gave to the docs of a
     * placeholder shard (see {@link #findUnmappedPlaceholders}), as seen by a shard that still sorts on the STRING placeholder.
     * Such a value stands for "no value for the field": parsing it as a string would not tie with the docs of the shard, which all
     * have no value, so a page would skip or repeat them. The values are the ones given by {@link #numericMissingValue}, as
     * numbers or, for infinities, as the strings they are rendered as in a response.
     */
    public static boolean isPlaceholderMissingValue(SortField sortField, Object value) {
        Object missingValue = stringMissingValue(sortField);
        if (missingValue != SortField.STRING_FIRST && missingValue != SortField.STRING_LAST) {
            return false;
        }
        boolean max = missingValue == SortField.STRING_LAST;
        if (value instanceof String string) {
            return string.equals(max ? "Infinity" : "-Infinity");
        }
        if (value instanceof Long || value instanceof Integer) {
            long number = ((Number) value).longValue();
            return number == (max ? Long.MAX_VALUE : Long.MIN_VALUE) || number == (max ? Integer.MAX_VALUE : Integer.MIN_VALUE);
        }
        if (value instanceof Double || value instanceof Float) {
            return ((Number) value).doubleValue() == (max ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
        }
        return false;
    }

    /**
     * Gives the docs of the placeholder shards the value that a doc without a value would have if the field was mapped as
     * {@code type}, so that they are ordered according to the {@code missing} option of the sort, like the other missing docs.
     */
    private static void rewriteUnmappedPlaceholders(
        List<TopFieldDocs> results,
        boolean[][] unmappedPlaceholders,
        int fieldIdx,
        SortField.Type type
    ) {
        for (int shard = 0; shard < results.size(); shard++) {
            if (unmappedPlaceholders[shard][fieldIdx]) {
                TopFieldDocs topFieldDocs = results.get(shard);
                boolean missingIsGreatest = stringMissingValue(topFieldDocs.fields[fieldIdx]) == SortField.STRING_LAST;
                Object missingValue = numericMissingValue(type, missingIsGreatest);
                for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
                    ((FieldDoc) scoreDoc).fields[fieldIdx] = missingValue;
                }
            }
        }
    }

    private static Object numericMissingValue(SortField.Type type, boolean max) {
        return switch (type) {
            case INT -> max ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            case LONG -> max ? Long.MAX_VALUE : Long.MIN_VALUE;
            case FLOAT -> max ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
            case DOUBLE -> max ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
            case SCORE, DOC, STRING, STRING_VAL, CUSTOM, REWRITEABLE -> throw new AssertionError("not a numeric sort type: " + type);
        };
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
        List<TopFieldDocs> results,
        boolean[][] unmappedPlaceholders,
        Set<Integer> fieldIdsWithMixedIntAndLongSorts
    ) {
        SortField[] newSortFields = sort.getSort();
        for (int fieldIdx : fieldIdsWithMixedIntAndLongSorts) {
            for (int shard = 0; shard < results.size(); shard++) {
                if (unmappedPlaceholders[shard][fieldIdx]) {
                    continue;
                }
                TopFieldDocs topDocs = results.get(shard);
                SortField[] sortFields = topDocs.fields;
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
    private static Sort rewriteSortAndResultsToDouble(Sort sort, List<TopFieldDocs> results, Set<Integer> fieldIdsWithMixedNumericSorts) {
        SortField[] newSortFields = sort.getSort();
        for (int fieldIdx : fieldIdsWithMixedNumericSorts) {
            // Rewrite the sort field to DOUBLE
            SortField originalField = newSortFields[fieldIdx];
            newSortFields[fieldIdx] = new SortField(
                originalField.getField(),
                SortField.Type.DOUBLE,
                originalField.getReverse(),
                originalField.getMissingValue()
            );

            // Convert all sort values to Double
            for (TopFieldDocs topDocs : results) {
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

    public static SortField.Type getType(SortField sortField) {
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
