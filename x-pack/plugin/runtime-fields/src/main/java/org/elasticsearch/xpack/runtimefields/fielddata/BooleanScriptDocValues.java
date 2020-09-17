/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.xpack.runtimefields.mapper.BooleanFieldScript;

import java.io.IOException;

public final class BooleanScriptDocValues extends AbstractSortedNumericDocValues {
    private final BooleanFieldScript script;
    private int cursor;

    BooleanScriptDocValues(BooleanFieldScript script) {
        this.script = script;
    }

    @Override
    public boolean advanceExact(int docId) {
        script.runForDoc(docId);
        cursor = 0;
        return script.trues() > 0 || script.falses() > 0;
    }

    @Override
    public long nextValue() throws IOException {
        // Emit all false values before all true values
        return cursor++ < script.falses() ? 0 : 1;
    }

    @Override
    public int docValueCount() {
        return script.trues() + script.falses();
    }
}
