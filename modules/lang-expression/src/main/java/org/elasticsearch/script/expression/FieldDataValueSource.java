/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link ValueSource} wrapper for field data.
 */
class FieldDataValueSource extends FieldDataBasedDoubleValuesSource {

    final MultiValueMode multiValueMode;

    protected FieldDataValueSource(IndexFieldData<?> fieldData, MultiValueMode multiValueMode) {
        super(fieldData);
        this.multiValueMode = Objects.requireNonNull(multiValueMode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldDataValueSource that = (FieldDataValueSource) o;

        if (fieldData.equals(that.fieldData) == false) return false;
        return multiValueMode == that.multiValueMode;

    }

    @Override
    public int hashCode() {
        int result = fieldData.hashCode();
        result = 31 * result + multiValueMode.hashCode();
        return result;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext leaf, DoubleValues scores) {
        LeafNumericFieldData leafData = (LeafNumericFieldData) fieldData.load(leaf);
        NumericDoubleValues docValues = multiValueMode.select(leafData.getDoubleValues());
        return new DoubleValues() {
            @Override
            public double doubleValue() throws IOException {
                return docValues.doubleValue();
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return docValues.advanceExact(doc);
            }
        };
    }

    @Override
    public String toString() {
        return "field(" + fieldData.getFieldName() + ")";
    }

}
