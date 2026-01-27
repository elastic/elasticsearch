/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.index.fielddata.ScriptDocValues;

public final class VersionScriptDocValues extends ScriptDocValues<String> {

    public VersionScriptDocValues(Supplier<String> supplier) {
        super(supplier);
    }

    public String getValue() {
        return get(0);
    }

    @Override
    public String get(int index) {
        if (supplier.size() == 0) {
            throw new IllegalStateException(
                "A document doesn't have a value for a field! " + "Use doc[<field>].size()==0 to check if a document is missing a field!"
            );
        }
        return supplier.getInternal(index);
    }

    @Override
    public int size() {
        return supplier.size();
    }
}
