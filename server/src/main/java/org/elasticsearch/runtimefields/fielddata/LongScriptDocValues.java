/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.runtimefields.mapper.AbstractLongFieldScript;

import java.util.Arrays;

public final class LongScriptDocValues extends AbstractSortedNumericDocValues {
    private final AbstractLongFieldScript script;
    private int cursor;

    LongScriptDocValues(AbstractLongFieldScript script) {
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
    public long nextValue() {
        return script.values()[cursor++];
    }

    @Override
    public int docValueCount() {
        return script.count();
    }
}
