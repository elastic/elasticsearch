/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class UnsignedLongScriptDocValues extends ScriptDocValues<Number> {
    private final SortedNumericDocValues in;
    private long[] values = new long[0];
    private int count;

    /**
     * Standard constructor.
     */
    public UnsignedLongScriptDocValues(SortedNumericDocValues in) {
        this.in = in;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (in.advanceExact(docId)) {
            resize(in.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = in.nextValue();
            }
        } else {
            resize(0);
        }
    }

    /**
     * Set the {@link #size()} and ensure that the {@link #values} array can
     * store at least that many entries.
     */
    protected void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
    }

    public Number getValue() {
        return get(0);
    }

    @Override
    public Number get(int index) {
        if (count == 0) {
            throw new IllegalStateException(
                "A document doesn't have a value for a field! Use doc[<field>].size()==0 to check if a document is missing a field!"
            );
        }
        return (Number) DocValueFormat.UNSIGNED_LONG_SHIFTED.format(values[index]);
    }

    @Override
    public int size() {
        return count;
    }
}
