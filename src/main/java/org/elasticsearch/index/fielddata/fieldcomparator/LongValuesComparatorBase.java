package org.elasticsearch.index.fielddata.fieldcomparator;
/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

import java.io.IOException;

abstract class LongValuesComparatorBase<T extends Number> extends NumberComparatorBase<T> {

    protected final IndexNumericFieldData<?> indexFieldData;
    protected final long missingValue;
    protected long bottom;
    protected LongValues readerValues;
    protected final SortMode sortMode;


    public LongValuesComparatorBase(IndexNumericFieldData<?> indexFieldData, long missingValue, SortMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.sortMode = sortMode;
    }

    @Override
    public final int compareBottom(int doc) throws IOException {
        long v2 = sortMode.getRelevantValue(readerValues, doc, missingValue);
        return compare(bottom, v2);
    }

    @Override
    public final int compareDocToValue(int doc, T valueObj) throws IOException {
        final long value = valueObj.longValue();
        long docValue = sortMode.getRelevantValue(readerValues, doc, missingValue);
        return compare(docValue, value);
    }

    static final int compare(long left, long right) {
        if (left > right) {
            return 1;
        } else if (left < right) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public final FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getLongValues();
        return this;
    }

    @Override
    public int compareBottomMissing() {
        return compare(bottom, missingValue);
    }
}
