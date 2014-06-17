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
package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

abstract class DoubleValuesComparatorBase<T extends Number> extends NumberComparatorBase<T> {

    protected final IndexNumericFieldData<?> indexFieldData;
    protected final double missingValue;
    protected double bottom;
    protected DoubleValues readerValues;
    protected final MultiValueMode sortMode;

    public DoubleValuesComparatorBase(IndexNumericFieldData<?> indexFieldData, double missingValue, MultiValueMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.sortMode = sortMode;
    }

    @Override
    public final int compareBottom(int doc) throws IOException {
        final double v2 = sortMode.getRelevantValue(readerValues, doc, missingValue);
        return compare(bottom, v2);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        return compare(top.doubleValue(), sortMode.getRelevantValue(readerValues, doc, missingValue));
    }

    @Override
    public final FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getDoubleValues();
        return this;
    }

    @Override
    public int compareBottomMissing() {
        return compare(bottom, missingValue);
    }

    @Override
    public int compareTopMissing() {
        return compare(top.doubleValue(), missingValue);
    }

    static final int compare(double left, double right) {
        return Double.compare(left, right);
    }
}
