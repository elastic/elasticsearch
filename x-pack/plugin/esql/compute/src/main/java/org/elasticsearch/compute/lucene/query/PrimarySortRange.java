/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.NumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Finds the doc range of a leaf that can match a filter on the leaf's primary index-sort field.
 * <p>
 *     Docs are stored in index-sort order, so a range filter on the primary sort field matches
 *     one contiguous doc range. Slicing the whole leaf into equal pieces ignores that: a narrow
 *     range falls into a few slices and only a few drivers do work. Slicing only this range
 *     spreads the work over all drivers.
 * </p>
 * <p>
 *     The range is a superset of the matching docs. It is rounded out to the block boundaries of
 *     the {@link DocValuesSkipper}. The query still runs on every slice, so results do not change.
 * </p>
 */
final class PrimarySortRange {
    /**
     * A doc range {@code [minDoc, maxDoc)} within a leaf.
     */
    record DocRange(int minDoc, int maxDoc) {
        int length() {
            return maxDoc - minDoc;
        }
    }

    private PrimarySortRange() {}

    /**
     * Collects the numeric range clauses that every matching doc must satisfy. It only looks
     * through wrappers that keep that guarantee. For any other query it collects nothing, which
     * gives a wider range and is always safe.
     */
    static List<NumericDocValuesRangeQuery> requiredRangeClauses(Query query) {
        List<NumericDocValuesRangeQuery> clauses = new ArrayList<>();
        collect(query, clauses);
        return clauses;
    }

    private static void collect(Query query, List<NumericDocValuesRangeQuery> clauses) {
        if (query instanceof NumericDocValuesRangeQuery range) {
            clauses.add(range);
        } else if (query instanceof ConstantScoreQuery constantScore) {
            collect(constantScore.getQuery(), clauses);
        } else if (query instanceof BoostQuery boost) {
            collect(boost.getQuery(), clauses);
        } else if (query instanceof BooleanQuery bool) {
            for (BooleanClause clause : bool) {
                if (clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER) {
                    collect(clause.query(), clauses);
                }
            }
        }
    }

    /**
     * Returns the doc range of {@code leaf} that can match the {@code clauses} on the primary sort
     * field, or {@code null} if it cannot say. It returns {@code null} when the leaf has no index
     * sort, no clause is on the primary sort field, or the field is not a dense single-valued
     * {@code INT} or {@code LONG} with a skipper. These are the same conditions under which
     * Lucene's {@code IndexSortSortedNumericDocValuesRangeQuery} uses the index sort.
     */
    static DocRange find(LeafReaderContext leaf, List<NumericDocValuesRangeQuery> clauses) throws IOException {
        LeafReader reader = leaf.reader();
        Sort indexSort = reader.getMetaData().sort();
        if (indexSort == null || indexSort.getSort().length == 0) {
            return null;
        }
        SortField primary = indexSort.getSort()[0];
        SortField.Type type = primary instanceof SortedNumericSortField numeric ? numeric.getNumericType() : primary.getType();
        if (type != SortField.Type.INT && type != SortField.Type.LONG) {
            return null;
        }
        // Clauses are ANDed, so their intersection is the range to look for.
        long lower = Long.MIN_VALUE;
        long upper = Long.MAX_VALUE;
        boolean found = false;
        for (NumericDocValuesRangeQuery clause : clauses) {
            if (clause.getField().equals(primary.getField())) {
                lower = Math.max(lower, clause.lowerValue());
                upper = Math.min(upper, clause.upperValue());
                found = true;
            }
        }
        if (found == false) {
            return null;
        }
        DocValuesSkipper skipper = reader.getDocValuesSkipper(primary.getField());
        if (skipper == null || skipper.docCount() != reader.maxDoc()) {
            return null;
        }
        if (DocValues.unwrapSingleton(DocValues.getSortedNumeric(reader, primary.getField())) == null) {
            return null;
        }
        return find(skipper, primary.getReverse(), lower, upper);
    }

    private static DocRange find(DocValuesSkipper skipper, boolean reverse, long lower, long upper) throws IOException {
        if (lower > upper || lower > skipper.maxValue() || upper < skipper.minValue()) {
            return new DocRange(0, 0);
        }
        int minDoc;
        int maxDoc;
        if (reverse) {
            // Values fall as doc ids rise. The first doc that can be <= upper starts the range.
            if (skipper.maxValue() <= upper) {
                minDoc = 0;
            } else {
                skipper.advance(Long.MIN_VALUE, upper);
                minDoc = skipper.minDocID(0);
            }
            if (skipper.minValue() >= lower) {
                maxDoc = skipper.docCount();
            } else {
                // The first doc below the range is somewhere in this block, so the range may reach the end of the block.
                skipper.advance(Long.MIN_VALUE, lower - 1);
                maxDoc = skipper.maxDocID(0) + 1;
            }
        } else {
            if (skipper.minValue() >= lower) {
                minDoc = 0;
            } else {
                skipper.advance(lower, Long.MAX_VALUE);
                minDoc = skipper.minDocID(0);
            }
            if (skipper.maxValue() <= upper) {
                maxDoc = skipper.docCount();
            } else {
                // The first doc above the range is somewhere in this block, so the range may reach the end of the block.
                skipper.advance(upper + 1, Long.MAX_VALUE);
                maxDoc = skipper.maxDocID(0) + 1;
            }
        }
        return new DocRange(minDoc, maxDoc);
    }
}
