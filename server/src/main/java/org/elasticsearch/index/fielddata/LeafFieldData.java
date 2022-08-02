/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

/**
 * The thread safe {@link org.apache.lucene.index.LeafReader} level cache of the data.
 */
public interface LeafFieldData extends Accountable, Releasable {

    /**
     * Returns an {@link DocValuesScriptFieldFactory} to access either
     * {@link ScriptDocValues} or {@link org.elasticsearch.script.field.Field}.
     */
    DocValuesScriptFieldFactory getScriptFieldFactory(String name);

    /**
     * Return a String representation of the values.
     */
    SortedBinaryDocValues getBytesValues();

    /**
     * Return a formatted representation of the values
     */
    default FormattedDocValues getFormattedValues(DocValueFormat format) {
        SortedBinaryDocValues values = getBytesValues();
        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return format.format(values.nextValue());
            }
        };
    }
}
