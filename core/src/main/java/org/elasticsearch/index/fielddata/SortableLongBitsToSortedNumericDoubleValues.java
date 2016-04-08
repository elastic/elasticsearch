/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;

/**
 * {@link SortedNumericDoubleValues} instance that wraps a {@link SortedNumericDocValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#sortableLongToDouble(long)}.
 */
final class SortableLongBitsToSortedNumericDoubleValues extends SortedNumericDoubleValues {

    private final SortedNumericDocValues values;

    SortableLongBitsToSortedNumericDoubleValues(SortedNumericDocValues values) {
        this.values = values;
    }

    @Override
    public void setDocument(int doc) {
        values.setDocument(doc);
    }

    @Override
    public double valueAt(int index) {
        return NumericUtils.sortableLongToDouble(values.valueAt(index));
    }

    @Override
    public int count() {
        return values.count();
    }

    /** Return the wrapped values. */
    public SortedNumericDocValues getLongValues() {
        return values;
    }

}
