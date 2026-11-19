/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SortedNumericDoubleValuesTests extends ESTestCase {

    public void testWrapSingleton() {
        double[] data = { 1.5, 2.5, 3.5 };

        NumericDocValues singletonDocValues = new AbstractNumericDocValues() {
            int currentDoc = -1;

            @Override
            public int docID() {
                return currentDoc;
            }

            @Override
            public boolean advanceExact(int target) {
                currentDoc = target;
                return target < data.length;
            }

            @Override
            public long longValue() {
                return NumericUtils.doubleToSortableLong(data[currentDoc]);
            }
        };

        SortedNumericDocValues docValues = DocValues.singleton(singletonDocValues);
        SortedNumericDoubleValues wrapped = SortedNumericDoubleValues.wrap(docValues);

        assertThat(wrapped.isSingleton(), equalTo(true));
        assertThat(wrapped.docIdIterator(), sameInstance(singletonDocValues));
        assertThat(wrapped.asDoubleValues(), notNullValue());
    }

    public void testWrapMultiValues() throws IOException {
        double[][] data = { { 1.1, 2.2 }, { 3.3 }, { 4.4, 5.5, 6.6 } };

        SortedNumericDocValues multiValues = new SortedNumericDocValues() {
            int currentDoc = -1;
            int valueIdx;

            @Override
            public int docID() {
                return currentDoc;
            }

            @Override
            public boolean advanceExact(int target) {
                currentDoc = target;
                valueIdx = 0;
                return target < data.length;
            }

            @Override
            public long nextValue() {
                return NumericUtils.doubleToSortableLong(data[currentDoc][valueIdx++]);
            }

            @Override
            public int docValueCount() {
                return data[currentDoc].length;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return data.length;
            }
        };

        SortedNumericDoubleValues wrapped = SortedNumericDoubleValues.wrap(multiValues);
        assertThat(wrapped.isSingleton(), equalTo(false));
        assertThat(wrapped.docIdIterator(), sameInstance(multiValues));
        assertThat(wrapped.asDoubleValues(), nullValue());
    }
}
