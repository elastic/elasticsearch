/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.xpack.runtimefields.AbstractLongScriptFieldScript;

import java.io.IOException;
import java.util.Arrays;

public final class ScriptLongDocValues extends AbstractSortedNumericDocValues {
    private final AbstractLongScriptFieldScript script;
    private long[] values;
    private int cursor;

    ScriptLongDocValues(AbstractLongScriptFieldScript script) {
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
    public long nextValue() throws IOException {
        return values[cursor++];
    }

    @Override
    public int docValueCount() {
        return values.length;
    }
}
