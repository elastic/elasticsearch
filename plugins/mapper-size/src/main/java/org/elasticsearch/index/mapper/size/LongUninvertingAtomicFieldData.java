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

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.FieldData;

final class LongUninvertingAtomicFieldData implements AtomicNumericFieldData {
    private final SortedNumericDocValues values;
    private final long bytesUsed;

    public LongUninvertingAtomicFieldData(NumericDocValues numeric, Bits docsWithField) {
        this.values = DocValues.singleton(numeric, docsWithField);
        long numBytes = 0;
        if (numeric instanceof Accountable) {
            numBytes += ((Accountable) numeric).ramBytesUsed();
        }
        if (docsWithField instanceof Accountable) {
            numBytes += ((Accountable) docsWithField).ramBytesUsed();
        }
        this.bytesUsed = numBytes;
    }

    private LongUninvertingAtomicFieldData(int maxDoc) {
        this.values = DocValues.emptySortedNumeric(maxDoc);
        this.bytesUsed = 0;
    }

    static LongUninvertingAtomicFieldData empty(int maxDoc) {
        return new LongUninvertingAtomicFieldData(maxDoc);
    }

    @Override
    public SortedNumericDocValues getLongValues() {
        return values;
    }

    @Override
    public long ramBytesUsed() {
        return bytesUsed;
    }

    @Override
    public final ScriptDocValues.Longs getScriptValues() {
        return new ScriptDocValues.Longs(getLongValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getLongValues());
    }

    @Override
    public final SortedNumericDoubleValues getDoubleValues() {
        return FieldData.castToDouble(getLongValues());
    }

    @Override
    public void close() {
    }
}
