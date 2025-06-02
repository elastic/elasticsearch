/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.document;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.lucene.search.XIndexSortSortedNumericDocValuesRangeQuery;

public final class NumericField {

    private NumericField() {
        // Utility class, no instantiation
    }

    public static Query newExactLongQuery(String field, long value) {
        return newRangeLongQuery(field, value, value);
    }

    public static Query newRangeLongQuery(String field, long lowerValue, long upperValue) {
        PointRangeQuery.checkArgs(field, lowerValue, upperValue);
        Query fallbackQuery = new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery(field, lowerValue, upperValue),
            SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue)
        );
        return new XIndexSortSortedNumericDocValuesRangeQuery(field, lowerValue, upperValue, fallbackQuery);
    }

    public static Query newExactIntQuery(String field, int value) {
        return newRangeIntQuery(field, value, value);
    }

    public static Query newRangeIntQuery(String field, int lowerValue, int upperValue) {
        PointRangeQuery.checkArgs(field, lowerValue, upperValue);
        Query fallbackQuery = new IndexOrDocValuesQuery(
            IntPoint.newRangeQuery(field, lowerValue, upperValue),
            SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue)
        );
        return new XIndexSortSortedNumericDocValuesRangeQuery(field, lowerValue, upperValue, fallbackQuery);
    }

}
