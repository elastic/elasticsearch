/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

public abstract class AbstractLeafOrdinalsFieldData implements LeafOrdinalsFieldData {

    private final ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory;

    protected AbstractLeafOrdinalsFieldData(ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory) {
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public final DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getOrdinalsValues(), name);
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getOrdinalsValues());
    }

    public static LeafOrdinalsFieldData empty(ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory) {
        return new AbstractLeafOrdinalsFieldData(toScriptFieldFactory) {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}

            @Override
            public SortedSetDocValues getOrdinalsValues() {
                return DocValues.emptySortedSet();
            }
        };
    }
}
