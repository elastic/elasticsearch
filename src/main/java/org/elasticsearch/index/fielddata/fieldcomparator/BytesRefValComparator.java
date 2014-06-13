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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Sorts by field's natural Term sort order.  All
 * comparisons are done using BytesRef.compareTo, which is
 * slow for medium to large result sets but possibly
 * very fast for very small results sets.
 */
public final class BytesRefValComparator extends NestedWrappableComparator<BytesRef> {

    private final IndexFieldData<?> indexFieldData;
    private final MultiValueMode sortMode;
    private final BytesRef missingValue;

    private final BytesRef[] values;
    private BytesRef bottom;
    private BytesRef top;
    private BytesValues docTerms;

    BytesRefValComparator(IndexFieldData<?> indexFieldData, int numHits, MultiValueMode sortMode, BytesRef missingValue) {
        this.sortMode = sortMode;
        values = new BytesRef[numHits];
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        return compareValues(val1, val2);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        BytesRef val2 = sortMode.getRelevantValue(docTerms, doc, missingValue);
        return compareValues(bottom, val2);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        return  top.compareTo(sortMode.getRelevantValue(docTerms, doc, missingValue));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        BytesRef relevantValue = sortMode.getRelevantValue(docTerms, doc, missingValue);
        if (relevantValue == missingValue) {
            values[slot] = missingValue;
        } else {
            if (values[slot] == null || values[slot] == missingValue) {
                values[slot] = new BytesRef();
            }
            values[slot].copyBytes(relevantValue);
        }
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        docTerms = indexFieldData.load(context).getBytesValues();
        return this;
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(BytesRef top) {
        this.top = top;
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return val1.compareTo(val2);
    }

    @Override
    public void missing(int slot) {
        values[slot] = missingValue;
    }

    @Override
    public int compareBottomMissing() {
        return compareValues(bottom, missingValue);
    }

    @Override
    public int compareTopMissing() {
        return compareValues(top, missingValue);
    }
}
