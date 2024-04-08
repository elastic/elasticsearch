/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;

import java.io.IOException;

/** {@link LeafFieldData} impl on top of Lucene's binary doc values. */
public class BinaryDVLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String field;

    public BinaryDVLeafFieldData(LeafReader reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        try {
            final BinaryDocValues values = DocValues.getBinary(reader, field);
            return FieldData.singleton(values);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return new DelegateDocValuesField(new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(getBytesValues())), name);
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public long ramBytesUsed() {
        return 0; // unknown
    }

}
