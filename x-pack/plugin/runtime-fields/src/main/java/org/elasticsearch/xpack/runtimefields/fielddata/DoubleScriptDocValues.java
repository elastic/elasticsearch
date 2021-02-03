/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.xpack.runtimefields.mapper.DoubleFieldScript;

import java.util.Arrays;

public final class DoubleScriptDocValues extends SortedNumericDoubleValues {
    private final DoubleFieldScript script;
    private int cursor;

    DoubleScriptDocValues(DoubleFieldScript script) {
        this.script = script;
    }

    @Override
    public boolean advanceExact(int docId) {
        script.runForDoc(docId);
        if (script.count() == 0) {
            return false;
        }
        Arrays.sort(script.values(), 0, script.count());
        cursor = 0;
        return true;
    }

    @Override
    public double nextValue() {
        return script.values()[cursor++];
    }

    @Override
    public int docValueCount() {
        return script.count();
    }
}
