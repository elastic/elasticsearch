/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.ScorerAware;

import java.io.IOException;

/**
 * {@link SortedNumericLongValues} instance that wraps a {@link SortedNumericDoubleValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#doubleToSortableLong(double)}.
 */
final class SortableLongBitsSortedNumericDocValues extends SortedNumericLongValues.SortedNumericDoubleWrapper implements ScorerAware {

    SortableLongBitsSortedNumericDocValues(SortedNumericDoubleValues values) {
        super(values);
    }

    @Override
    public long nextValue() throws IOException {
        return NumericUtils.doubleToSortableLong(getDoubleValues().nextValue());
    }

    @Override
    public void setScorer(Scorable scorer) {
        if (getDoubleValues() instanceof ScorerAware aware) {
            aware.setScorer(scorer);
        }
    }
}
