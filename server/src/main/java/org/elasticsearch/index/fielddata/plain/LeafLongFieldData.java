/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

/**
 * Specialization of {@link LeafNumericFieldData} for integers.
 */
public abstract class LeafLongFieldData implements LeafNumericFieldData {

    private final long ramBytesUsed;

    protected LeafLongFieldData(long ramBytesUsed) {
        this.ramBytesUsed = ramBytesUsed;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    /*@Override
    public final DocValuesField<?> getScriptField(String name) {
        switch (numericType) {
            // for now, dates and nanoseconds are treated the same, which also means, that the precision is only on millisecond level
            case DATE:
                return new DelegateDocValuesField(new ScriptDocValues.Dates(getLongValues(), false), name);
            case DATE_NANOSECONDS:
                assert this instanceof SortedNumericIndexFieldData.NanoSecondFieldData;
                return new DelegateDocValuesField(
                    new ScriptDocValues.Dates(((SortedNumericIndexFieldData.NanoSecondFieldData) this).getLongValuesAsNanos(), true),
                    name
                );
            case BOOLEAN:
                return new DelegateDocValuesField(new ScriptDocValues.Booleans(getLongValues()), name);
            default:
                return new DelegateDocValuesField(new ScriptDocValues.Longs(getLongValues()), name);
        }
    }*/

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getLongValues());
    }

    @Override
    public final SortedNumericDoubleValues getDoubleValues() {
        return FieldData.castToDouble(getLongValues());
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        SortedNumericDocValues values = getLongValues();
        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int docValueCount() throws IOException {
                return values.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return format.format(values.nextValue());
            }
        };
    }

    @Override
    public void close() {}
}
