/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Thread-safe utility class that allows to get per-segment values via the
 * {@link #load(LeafReaderContext)} method.
 */
public interface IndexFieldData<FD extends LeafFieldData> {

    /**
     * The field name.
     */
    String getFieldName();

    /**
     * The ValuesSourceType of the underlying data.  It's possible for fields that use the same IndexFieldData implementation to have
     * different ValuesSourceTypes, such as in the case of Longs and Dates.
     */
    ValuesSourceType getValuesSourceType();

    /**
     * Loads the atomic field data for the reader, possibly cached.
     */
    FD load(LeafReaderContext context);

    /**
     * Loads directly the atomic field data for the reader, ignoring any caching involved.
     */
    FD loadDirect(LeafReaderContext context) throws Exception;

    /**
     * Returns the {@link SortField} to use for sorting.
     */
    SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse);

    /**
     * Build a sort implementation specialized for aggregations.
     */
    BucketedSort newBucketedSort(
        BigArrays bigArrays,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    );

    // we need this extended source we we have custom comparators to reuse our field data
    // in this case, we need to reduce type that will be used when search results are reduced
    // on another node (we don't have the custom source them...)
    abstract class XFieldComparatorSource extends FieldComparatorSource {

        protected final MultiValueMode sortMode;
        protected final Object missingValue;
        protected final Nested nested;

        public XFieldComparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
            this.sortMode = sortMode;
            this.missingValue = missingValue;
            this.nested = nested;
        }

        public MultiValueMode sortMode() {
            return this.sortMode;
        }

        public Nested nested() {
            return this.nested;
        }

        /**
         * Simple wrapper class around a filter that matches parent documents
         * and a filter that matches child documents. For every root document R,
         * R will be in the parent filter and its children documents will be the
         * documents that are contained in the inner set between the previous
         * parent + 1, or 0 if there is no previous parent, and R (excluded).
         */
        public static class Nested {

            private final BitSetProducer rootFilter;
            private final Query innerQuery;
            private final NestedSortBuilder nestedSort;
            private final IndexSearcher searcher;

            public Nested(BitSetProducer rootFilter, Query innerQuery, NestedSortBuilder nestedSort, IndexSearcher searcher) {
                this.rootFilter = rootFilter;
                this.innerQuery = innerQuery;
                this.nestedSort = nestedSort;
                this.searcher = searcher;
            }

            public Query getInnerQuery() {
                return innerQuery;
            }

            public NestedSortBuilder getNestedSort() {
                return nestedSort;
            }

            /**
             * Get a {@link BitDocIdSet} that matches the root documents.
             */
            public BitSet rootDocs(LeafReaderContext ctx) throws IOException {
                return rootFilter.getBitSet(ctx);
            }

            /**
             * Get a {@link DocIdSet} that matches the inner documents.
             */
            public DocIdSetIterator innerDocs(LeafReaderContext ctx) throws IOException {
                Weight weight = searcher.createWeight(searcher.rewrite(innerQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
                Scorer s = weight.scorer(ctx);
                return s == null ? null : s.iterator();
            }
        }

        /** Whether missing values should be sorted first. */
        public static final boolean sortMissingFirst(Object missingValue) {
            return "_first".equals(missingValue);
        }

        /** Whether missing values should be sorted last, this is the default. */
        public static final boolean sortMissingLast(Object missingValue) {
            return missingValue == null || "_last".equals(missingValue);
        }

        /** Return the missing object value according to the reduced type of the comparator. */
        public Object missingObject(Object missingValue, boolean reversed) {
            if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                final boolean min = sortMissingFirst(missingValue) ^ reversed;
                return switch (reducedType()) {
                    case INT -> min ? Integer.MIN_VALUE : Integer.MAX_VALUE;
                    case LONG -> min ? Long.MIN_VALUE : Long.MAX_VALUE;
                    case FLOAT -> min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
                    case DOUBLE -> min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                    case STRING, STRING_VAL -> null;
                    default -> throw new UnsupportedOperationException("Unsupported reduced type: " + reducedType());
                };
            } else {
                switch (reducedType()) {
                    case INT:
                        if (missingValue instanceof Number) {
                            return ((Number) missingValue).intValue();
                        } else {
                            return Integer.parseInt(missingValue.toString());
                        }
                    case LONG:
                        if (missingValue instanceof Number) {
                            return ((Number) missingValue).longValue();
                        } else {
                            return Long.parseLong(missingValue.toString());
                        }
                    case FLOAT:
                        if (missingValue instanceof Number) {
                            return ((Number) missingValue).floatValue();
                        } else {
                            return Float.parseFloat(missingValue.toString());
                        }
                    case DOUBLE:
                        if (missingValue instanceof Number) {
                            return ((Number) missingValue).doubleValue();
                        } else {
                            return Double.parseDouble(missingValue.toString());
                        }
                    case STRING:
                    case STRING_VAL:
                        if (missingValue instanceof BytesRef) {
                            return missingValue;
                        } else if (missingValue instanceof byte[]) {
                            return new BytesRef((byte[]) missingValue);
                        } else {
                            return new BytesRef(missingValue.toString());
                        }
                    default:
                        throw new UnsupportedOperationException("Unsupported reduced type: " + reducedType());
                }
            }
        }

        public abstract SortField.Type reducedType();

        /**
         * Return a missing value that is understandable by {@link SortField#setMissingValue(Object)}.
         * Most implementations return null because they already replace the value at the fielddata level.
         * However this can't work in case of strings since there is no such thing as a string which
         * compares greater than any other string, so in that case we need to return
         * {@link SortField#STRING_FIRST} or {@link SortField#STRING_LAST} so that the coordinating node
         * knows how to deal with null values.
         */
        public Object missingValue(boolean reversed) {
            return null;
        }

        /**
         * Create a {@linkplain BucketedSort} which is useful for sorting inside of aggregations.
         */
        public abstract BucketedSort newBucketedSort(
            BigArrays bigArrays,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        );
    }

    interface Builder {

        IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService);
    }

    interface Global<FD extends LeafFieldData> extends IndexFieldData<FD> {

        IndexFieldData<FD> loadGlobal(DirectoryReader indexReader);

        IndexFieldData<FD> loadGlobalDirect(DirectoryReader indexReader) throws Exception;

    }
}
