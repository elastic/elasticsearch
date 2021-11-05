/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.Doubles;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

public interface ToScriptField<T> {

    DocValuesField<?> getScriptField(T docValues, String name);

    ToScriptField<SortedNumericDocValues> _VERSION = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDocValues> _SEQNO = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDocValues> BOOLEAN = BooleanDocValuesField::new;
    ToScriptField<SortedNumericDocValues> BYTE = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDocValues> SHORT = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDocValues> INT = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDocValues> LONG = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDoubleValues> HALF_FLOAT = (dv, n) -> new DelegateDocValuesField(new Doubles(dv), n);
    ToScriptField<SortedNumericDoubleValues> FLOAT = (dv, n) -> new DelegateDocValuesField(new Doubles(dv), n);
    ToScriptField<SortedNumericDoubleValues> SCALED_FLOAT = (dv, n) -> new DelegateDocValuesField(new Doubles(dv), n);
    ToScriptField<SortedNumericDoubleValues> DOUBLE = (dv, n) -> new DelegateDocValuesField(new Doubles(dv), n);
    ToScriptField<SortedNumericDocValues> DATE_MILLIS = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
    ToScriptField<SortedNumericDocValues> DATE_NANOS = (dv, n) -> new DelegateDocValuesField(new Longs(dv), n);
}
