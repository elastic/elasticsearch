/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;

public class IntegerDocValuesSource implements FieldDocValuesSource {

    protected final IntegerDocValuesSupplier supplier;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    protected ScriptDocValues.Longs sdv = null;

    public IntegerDocValuesSource(SortedNumericDocValues docValues) {
        this.supplier = new IntegerDocValuesSupplier(docValues);
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        supplier.setNextDocId(docId);
    }

    @Override
    public Field<?> toScriptField(String name) {
        throw new UnsupportedOperationException("doc values is unsupported for field [" + name + "]");
    }

    @Override
    public ScriptDocValues.Longs toScriptDocValues() {
        if (sdv == null) {
            sdv = new ScriptDocValues.Longs(supplier);
        }

        return sdv;
    }
}
