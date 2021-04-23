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
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;


public abstract class AbstractLeafOrdinalsFieldData implements LeafOrdinalsFieldData {

    public static final Function<SortedSetDocValues, ScriptDocValues<?>> DEFAULT_SCRIPT_FUNCTION =
            ((Function<SortedSetDocValues, SortedBinaryDocValues>) FieldData::toString)
            .andThen(ScriptDocValues.Strings::new);

    private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;

    protected AbstractLeafOrdinalsFieldData(Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction) {
        this.scriptFunction = scriptFunction;
    }

    @Override
    public final ScriptDocValues<?> getScriptValues() {
        return scriptFunction.apply(getOrdinalsValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getOrdinalsValues());
    }

    public static LeafOrdinalsFieldData empty() {
        return new AbstractLeafOrdinalsFieldData(DEFAULT_SCRIPT_FUNCTION) {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public SortedSetDocValues getOrdinalsValues() {
                return DocValues.emptySortedSet();
            }
        };
    }
}
