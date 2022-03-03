/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.ScorerAware;

import java.io.IOException;

/**
 * {@link SortedNumericDocValues} instance that wraps a {@link SortedNumericDoubleValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#doubleToSortableLong(double)}.
 */
final class SortableLongBitsSortedNumericDocValues extends AbstractSortedNumericDocValues implements ScorerAware {

    private final SortedNumericDoubleValues values;

    SortableLongBitsSortedNumericDocValues(SortedNumericDoubleValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public long nextValue() throws IOException {
        return NumericUtils.doubleToSortableLong(values.nextValue());
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    /** Return the wrapped values. */
    public SortedNumericDoubleValues getDoubleValues() {
        return values;
    }

    @Override
    public void setScorer(Scorable scorer) {
        if (values instanceof ScorerAware aware) {
            aware.setScorer(scorer);
        }
    }
}
