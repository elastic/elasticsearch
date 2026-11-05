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
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * The values a doc must have on the primary sort field, both ends inclusive.
     */
    private record Bounds(long lower, long upper) {
        Bounds intersect(Bounds other) {
            return new Bounds(Math.max(lower, other.lower), Math.min(upper, other.upper));
        }
    }

    private PrimarySortRange() {}

    /**
     * Finds the doc range of each leaf in {@code leaves} that can match the range clauses of {@code query} on the
     * primary sort field. {@code leaves} must all come from one shard, because they share one index sort. A leaf is
     * missing from the result if it cannot say which range matches, see {@link #findInLeaf}. The result is empty if
     * the shard has no usable primary sort or the query has no required range on it.
     */
    static Map<LeafReaderContext, DocRange> find(List<LeafReaderContext> leaves, Query query) {
        if (leaves.isEmpty()) {
            return Map.of();
        }
        // Lucene rejects a segment whose sort differs from the rest of the index, so any leaf gives the sort of the shard.
        SortField primary = supportedPrimarySort(leaves.getFirst());
        if (primary == null) {
            return Map.of();
        }
        Bounds bounds = requiredBounds(query, primary.getField());
        if (bounds == null) {
            return Map.of();
        }
        Map<LeafReaderContext, DocRange> ranges = new IdentityHashMap<>();
        try {
            for (LeafReaderContext leaf : leaves) {
                DocRange range = findInLeaf(leaf, primary, bounds);
                if (range != null) {
                    ranges.put(leaf, range);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ranges;
    }

    /**
     * The first sort field of the leaf's index sort, if it is an {@code INT} or {@code LONG}. These are the types that
     * Lucene's {@code IndexSortSortedNumericDocValuesRangeQuery} supports. Returns {@code null} if the leaf has no
     * index sort or the first field has another type.
     */
    @Nullable
    private static SortField supportedPrimarySort(LeafReaderContext leaf) {
        Sort indexSort = leaf.reader().getMetaData().sort();
        if (indexSort == null || indexSort.getSort().length == 0) {
            return null;
        }
        SortField primary = indexSort.getSort()[0];
        SortField.Type type = primary instanceof SortedNumericSortField numeric ? numeric.getNumericType() : primary.getType();
        if (type != SortField.Type.INT && type != SortField.Type.LONG) {
            return null;
        }
        return primary;
    }

    /**
     * The values on {@code field} that every matching doc must have, or {@code null} if the query does not require a range
     * on it. Several required clauses are intersected as they are found. Only wrappers that keep the guarantee are looked
     * through. For any other query the result is {@code null}, which gives a wider range and is always safe.
     */
    @Nullable
    private static Bounds requiredBounds(Query query, String field) {
        if (query instanceof NumericDocValuesRangeQuery range) {
            return range.getField().equals(field) ? new Bounds(range.lowerValue(), range.upperValue()) : null;
        }
        if (query instanceof ConstantScoreQuery constantScore) {
            return requiredBounds(constantScore.getQuery(), field);
        }
        if (query instanceof BoostQuery boost) {
            return requiredBounds(boost.getQuery(), field);
        }
        if (query instanceof BooleanQuery bool) {
            Bounds bounds = null;
            for (BooleanClause clause : bool) {
                if (clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER) {
                    Bounds clauseBounds = requiredBounds(clause.query(), field);
                    if (clauseBounds != null) {
                        bounds = bounds == null ? clauseBounds : bounds.intersect(clauseBounds);
                    }
                }
            }
            return bounds;
        }
        return null;
    }

    /**
     * Returns the doc range of {@code leaf} that can have values in {@code bounds} on the primary sort field, or
     * {@code null} if it cannot say. It returns {@code null} when the field is not a dense single-valued field with a
     * skipper in this leaf. These are the same conditions under which Lucene's
     * {@code IndexSortSortedNumericDocValuesRangeQuery} uses the index sort.
     */
    @Nullable
    private static DocRange findInLeaf(LeafReaderContext leaf, SortField primary, Bounds bounds) throws IOException {
        LeafReader reader = leaf.reader();
        DocValuesSkipper skipper = reader.getDocValuesSkipper(primary.getField());
        if (skipper == null || skipper.docCount() != reader.maxDoc()) {
            return null;
        }
        if (DocValues.unwrapSingleton(DocValues.getSortedNumeric(reader, primary.getField())) == null) {
            return null;
        }
        return findInSkipper(skipper, primary.getReverse(), bounds.lower(), bounds.upper());
    }

    private static DocRange findInSkipper(DocValuesSkipper skipper, boolean reverse, long lower, long upper) throws IOException {
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
