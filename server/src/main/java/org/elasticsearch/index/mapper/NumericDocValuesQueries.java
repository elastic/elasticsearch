/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.columnar.ColumnarNumericRangeQuery;

import java.util.function.Supplier;

/**
 * Builds the queries that a numeric field answers from its doc values, for the layout those doc values are stored in. Both
 * take bounds in the sortable-long encoding the field writes its values in.
 */
public enum NumericDocValuesQueries {
    /** Doc values stored as {@code SORTED_NUMERIC}. */
    SORTED_NUMERIC {
        @Override
        public Query rangeQuery(String field, long lowerValue, long upperValue) {
            return SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);
        }

        @Override
        public Query pointsAndDocValuesQuery(
            String field,
            Query pointQuery,
            long lowerValue,
            long upperValue,
            Supplier<Query> luceneFieldQuery
        ) {
            return luceneFieldQuery.get();
        }
    },
    /** Doc values stored by the ColumNAR codec, see {@link NumberFieldMapper.DocValuesDiskFormat#COLUMNAR_BINARY}. */
    COLUMNAR {
        @Override
        public Query rangeQuery(String field, long lowerValue, long upperValue) {
            return new ColumnarNumericRangeQuery(field, lowerValue, upperValue);
        }

        @Override
        public Query pointsAndDocValuesQuery(
            String field,
            Query pointQuery,
            long lowerValue,
            long upperValue,
            Supplier<Query> luceneFieldQuery
        ) {
            return new IndexOrDocValuesQuery(pointQuery, rangeQuery(field, lowerValue, upperValue));
        }
    };

    /**
     * Matches documents with a value in {@code [lowerValue, upperValue]}, both inclusive.
     */
    public abstract Query rangeQuery(String field, long lowerValue, long upperValue);

    /**
     * Matches documents with a value in {@code [lowerValue, upperValue]} for a field indexed with points as well as doc values,
     * from whichever of the two is cheaper.
     *
     * @param pointQuery       the same query over the points
     * @param luceneFieldQuery the same query from the Lucene field that writes both points and {@code SORTED_NUMERIC}, such as
     *                         {@code LongField.newRangeQuery}, which reads those doc values itself
     */
    public abstract Query pointsAndDocValuesQuery(
        String field,
        Query pointQuery,
        long lowerValue,
        long upperValue,
        Supplier<Query> luceneFieldQuery
    );
}
