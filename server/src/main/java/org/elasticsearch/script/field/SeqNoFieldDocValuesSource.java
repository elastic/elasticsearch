/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;

public class SeqNoFieldDocValuesSource extends LongDocValuesSource {

    protected SeqNoField field = null;

    public SeqNoFieldDocValuesSource(SortedNumericDocValues docValues) {
        super(docValues);
    }

    @Override
    public SeqNoField toScriptField(String name) {
        if (field == null) {
            field = new SeqNoField(name, supplier);
        }

        return field;
    }
}
