/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;

public class UnsignedLongScriptDocValues extends ScriptDocValues<Long> {

    private final UnsignedLongDocValuesField unsignedLongDocValuesField;

    /**
     * Standard constructor.
     */
    public UnsignedLongScriptDocValues(UnsignedLongDocValuesField unsignedLongDocValuesField) {
        this.unsignedLongDocValuesField = unsignedLongDocValuesField;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long getValue() {
        throwIfEmpty();
        return unsignedLongDocValuesField.getValue(0L); // default is ignored
    }

    @Override
    public Long get(int index) {
        throwIfEmpty();
        return unsignedLongDocValuesField.getValue(0L); // default is ignored
    }

    @Override
    public int size() {
        return unsignedLongDocValuesField.size();
    }
}
