/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.runtimefields.mapper.StringFieldScript;

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
