/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesField;

final class StringBinaryDVLeafFieldData extends AbstractBinaryDVLeafFieldData {
    StringBinaryDVLeafFieldData(BinaryDocValues values) {
        super(values);
    }

    @Override
    public DocValuesField<?> getScriptField(String name) {
        return new DelegateDocValuesField(new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(getBytesValues())), name);
    }
}
