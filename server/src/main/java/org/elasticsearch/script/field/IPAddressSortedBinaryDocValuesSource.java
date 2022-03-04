/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

public class IPAddressSortedBinaryDocValuesSource implements FieldDocValuesSource {

    protected final IpAddressSortedBinaryDocValuesSupplier supplier;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    protected ScriptDocValues.Strings sdv = null;

    public IPAddressSortedBinaryDocValuesSource(SortedBinaryDocValues docValues) {
        this.supplier = new IpAddressSortedBinaryDocValuesSupplier(docValues);
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
    public ScriptDocValues.Strings toScriptDocValues() {
        if (sdv == null) {
            sdv = new ScriptDocValues.Strings(supplier);
        }

        return sdv;
    }
}
