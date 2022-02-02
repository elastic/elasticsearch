/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.index.fielddata.ScriptDocValues;

public class UnsignedLongScriptDocValues extends ScriptDocValues<Long> {

    public UnsignedLongScriptDocValues(Supplier<Long> supplier) {
        super(supplier);
    }

    public long getValue() {
        return get(0);
    }

    @Override
    public Long get(int index) {
        throwIfEmpty();
        return supplier.getInternal(index);
    }

    @Override
    public int size() {
        return supplier.size();
    }
}
