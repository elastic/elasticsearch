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
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

abstract class LongValuesComparatorBase<T extends Number> extends NumberComparatorBase<T> {

    protected final IndexNumericFieldData<?> indexFieldData;
    protected final long missingValue;
    protected long bottom;
    protected LongValues readerValues;
    protected final MultiValueMode sortMode;


    public LongValuesComparatorBase(IndexNumericFieldData<?> indexFieldData, long missingValue, MultiValueMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.sortMode = sortMode;
    }

    @Override
    public final int compareBottom(int doc) throws IOException {
        long v2 = sortMode.getRelevantValue(readerValues, doc, missingValue);
        return Long.compare(bottom, v2);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        return Long.compare(top.longValue(), sortMode.getRelevantValue(readerValues, doc, missingValue));
    }

    @Override
    public final FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
        readerValues = indexFieldData.load(context).getLongValues();
        return this;
    }

    @Override
    public int compareBottomMissing() {
        return Long.compare(bottom, missingValue);
    }

    @Override
    public int compareTopMissing() {
        return Long.compare(top.longValue(), missingValue);
    }
}
