/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;


/**
 * Specialization of {@link LeafNumericFieldData} for floating-point numerics.
 */
public abstract class LeafDoubleFieldData implements LeafNumericFieldData {

    private final long ramBytesUsed;

    protected LeafDoubleFieldData(long ramBytesUsed) {
        this.ramBytesUsed = ramBytesUsed;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public final ScriptDocValues<Double> getScriptValues() {
        return new ScriptDocValues.Doubles(getDoubleValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getDoubleValues());
    }

    @Override
    public final SortedNumericDocValues getLongValues() {
        return FieldData.castToLong(getDoubleValues());
    }

    public static LeafNumericFieldData empty(final int maxDoc) {
        return new LeafDoubleFieldData(0) {

            @Override
            public SortedNumericDoubleValues getDoubleValues() {
                return FieldData.emptySortedNumericDoubles();
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

        };
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        SortedNumericDoubleValues values = getDoubleValues();
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
    public void close() {
    }

}
