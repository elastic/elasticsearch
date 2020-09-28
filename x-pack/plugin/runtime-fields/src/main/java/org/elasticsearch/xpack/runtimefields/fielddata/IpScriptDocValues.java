/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.xpack.runtimefields.mapper.IpFieldScript;

import java.util.Arrays;

public final class IpScriptDocValues extends SortedBinaryDocValues {
    private final IpFieldScript script;
    private int cursor;

    IpScriptDocValues(IpFieldScript script) {
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
    public BytesRef nextValue() {
        return script.values()[cursor++];
    }

    @Override
    public int docValueCount() {
        return script.count();
    }
}
