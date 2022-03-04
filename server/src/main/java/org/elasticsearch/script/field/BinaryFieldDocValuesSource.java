/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

public class BinaryFieldDocValuesSource extends BinaryDocValuesSource {

    protected BinaryField field = null;

    public BinaryFieldDocValuesSource(SortedBinaryDocValues docValues) {
        super(docValues);
    }

    @Override
    public BinaryField toScriptField(String name) {
        if (field == null) {
            field = new BinaryField(name, supplier);
        }

        return field;
    }
}
