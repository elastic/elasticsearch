/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;

import java.io.IOException;
import java.util.Arrays;

public final class ScriptDoubleDocValues extends SortedNumericDoubleValues {
    private final DoubleScriptFieldScript script;
    private double[] values;
    private int cursor;

    ScriptDoubleDocValues(DoubleScriptFieldScript script) {
        this.script = script;
    }

    @Override
    public boolean advanceExact(int docId) {
        values = script.runForDoc(docId);
        if (values.length == 0) {
            return false;
        }
        Arrays.sort(values);
        cursor = 0;
        return true;
    }

    @Override
    public double nextValue() throws IOException {
        return values[cursor++];
    }

    @Override
    public int docValueCount() {
        return values.length;
    }
}
