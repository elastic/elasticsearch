/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;

/**
 * A ValueSource to create FunctionValues to get the count of the number of values in a field for a document.
 */
final class CountMethodValueSource extends FieldDataBasedDoubleValuesSource {

    CountMethodValueSource(IndexFieldData<?> fieldData) {
        super(fieldData);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) {
        LeafNumericFieldData leafData = (LeafNumericFieldData) fieldData.load(ctx);
        final SortedNumericDoubleValues values = leafData.getDoubleValues();
        return new DoubleValues() {
            @Override
            public double doubleValue() {
                return values.docValueCount();
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountMethodValueSource that = (CountMethodValueSource) o;
        return fieldData.equals(that.fieldData);
    }

    @Override
    public String toString() {
        return "count: field(" + fieldData.getFieldName() + ")";
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode() + fieldData.hashCode();
    }

}
