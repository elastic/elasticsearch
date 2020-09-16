/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.xpack.runtimefields.DoubleScript;

import java.io.IOException;
import java.util.Arrays;

public final class ScriptDoubleDocValues extends SortedNumericDoubleValues {
    private final DoubleScript script;
    private int cursor;

    ScriptDoubleDocValues(DoubleScript script) {
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
    public double nextValue() throws IOException {
        return script.values()[cursor++];
    }

    @Override
    public int docValueCount() {
        return script.count();
    }
}
