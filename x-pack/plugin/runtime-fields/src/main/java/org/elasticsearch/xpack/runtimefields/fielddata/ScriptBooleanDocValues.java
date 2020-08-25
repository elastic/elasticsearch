/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.xpack.runtimefields.BooleanScriptFieldScript;

import java.io.IOException;

public final class ScriptBooleanDocValues extends AbstractSortedNumericDocValues {
    private final BooleanScriptFieldScript script;
    private int trues;
    private int falses;
    private int cursor;

    ScriptBooleanDocValues(BooleanScriptFieldScript script) {
        this.script = script;
    }

    @Override
    public boolean advanceExact(int docId) {
        boolean[] values = script.runForDoc(docId);
        if (values.length == 0) {
            return false;
        }
        trues = falses = cursor = 0;
        /*
         * We need to emit all falses before all trues so instead of
         * sorting we count them all.
         */
        for (boolean v : values) {
            if (v) {
                trues++;
            } else {
                falses++;
            }
        }
        return true;
    }

    @Override
    public long nextValue() throws IOException {
        // Emit all false values before all true values
        return cursor++ < falses ? 0 : 1;
    }

    @Override
    public int docValueCount() {
        return trues + falses;
    }
}
