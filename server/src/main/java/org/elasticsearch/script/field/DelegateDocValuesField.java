/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Iterator;

/**
 * A default {@link Field} to provide {@code ScriptDocValues} for fields
 * that are not supported by the script fields api.
 */
public class DelegateDocValuesField extends AbstractScriptFieldFactory<Object> implements Field<Object>, DocValuesScriptFieldFactory {

    private final ScriptDocValues<?> scriptDocValues;
    private final String name;

    public DelegateDocValuesField(ScriptDocValues<?> scriptDocValues, String name) {
        // Suppliers provided via ScriptDocValues should never be a Field
        // as we expect DelegateDocValuesField to only support old-style ScriptDocValues
        assert scriptDocValues.getSupplier() instanceof Field == false;
        this.scriptDocValues = scriptDocValues;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        scriptDocValues.getSupplier().setNextDocId(docId);
    }

    @Override
    public ScriptDocValues<?> toScriptDocValues() {
        return scriptDocValues;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
}
