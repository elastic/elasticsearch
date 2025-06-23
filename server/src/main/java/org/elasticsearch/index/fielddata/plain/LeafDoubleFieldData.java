/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

/**
 * Specialization of {@link LeafNumericFieldData} for floating-point numerics.
 */
public abstract class LeafDoubleFieldData implements LeafNumericFieldData {

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getDoubleValues());
    }

    @Override
    public final SortedNumericDocValues getLongValues() {
        return FieldData.castToLong(getDoubleValues());
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        SortedNumericDoubleValues values = getDoubleValues();
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
