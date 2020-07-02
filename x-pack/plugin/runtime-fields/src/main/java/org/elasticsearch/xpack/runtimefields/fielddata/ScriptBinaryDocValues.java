/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

public final class ScriptBinaryDocValues extends SortingBinaryDocValues {

    private final StringScriptFieldScript script;
    private final ScriptBinaryFieldData.ScriptBinaryResult scriptBinaryResult;

    ScriptBinaryDocValues(StringScriptFieldScript script, ScriptBinaryFieldData.ScriptBinaryResult scriptBinaryResult) {
        this.script = script;
        this.scriptBinaryResult = scriptBinaryResult;
    }

    @Override
    public boolean advanceExact(int doc) {
        script.setDocId(doc);
        script.execute();

        count = scriptBinaryResult.getResult().size();
        if (count == 0) {
            grow();
            return false;
        }

        int i = 0;
        for (String value : scriptBinaryResult.getResult()) {
            grow();
            values[i++].copyChars(value);
        }
        sort();
        return true;
    }
}
