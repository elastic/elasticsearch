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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SortedNumericLongValuesTests extends ESTestCase {

    public void testWrapSingleton() throws IOException {
        long[] data = { 10L, 20L, 30L };

        NumericDocValues numericDocValues = new AbstractNumericDocValues() {
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
                return data[currentDoc];
            }
        };

        SortedNumericDocValues singleton = DocValues.singleton(numericDocValues);
        SortedNumericLongValues wrapped = SortedNumericLongValues.wrap(singleton);

        assertThat(wrapped.isSingleton(), equalTo(true));
        assertThat(wrapped.docIdIterator(), sameInstance(numericDocValues));
        assertThat(SortedNumericLongValues.unwrapSingleton(wrapped), notNullValue());
    }

    public void testWrapMultiValued() throws IOException {
        long[][] data = { { 1L, 2L }, { 3L }, { 4L, 5L, 6L } };

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
            public long nextValue() throws IOException {
                return data[currentDoc][valueIdx++];
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

        SortedNumericLongValues wrapped = SortedNumericLongValues.wrap(multiValues);

        assertThat(wrapped.isSingleton(), equalTo(false));
        assertThat(wrapped.docIdIterator(), sameInstance(multiValues));
        assertThat(SortedNumericLongValues.unwrapSingleton(wrapped), nullValue());
    }
}
