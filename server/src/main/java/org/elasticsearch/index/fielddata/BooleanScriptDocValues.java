/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.script.BooleanFieldScript;

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
    public long nextValue() {
        // Emit all false values before all true values
        return cursor++ < script.falses() ? 0 : 1;
    }

    @Override
    public int docValueCount() {
        return script.trues() + script.falses();
    }
}
