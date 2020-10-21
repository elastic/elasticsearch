/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.List;

public final class StringScriptDocValues extends SortingBinaryDocValues {
    private final StringFieldScript script;

    StringScriptDocValues(StringFieldScript script) {
        this.script = script;
    }

    @Override
    public boolean advanceExact(int docId) {
        List<String> results = script.resultsForDoc(docId);
        count = results.size();
        if (count == 0) {
            return false;
        }

        grow();
        int i = 0;
        for (String value : results) {
            values[i++].copyChars(value);
        }
        sort();
        return true;
    }
}
