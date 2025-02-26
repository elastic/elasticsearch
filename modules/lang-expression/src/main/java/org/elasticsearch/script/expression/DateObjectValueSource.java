/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.function.ToIntFunction;

/** Extracts a portion of a date field with Java time */
class DateObjectValueSource extends FieldDataValueSource {

    final String methodName;
    final ToIntFunction<ZonedDateTime> function;

    DateObjectValueSource(
        IndexFieldData<?> indexFieldData,
        MultiValueMode multiValueMode,
        String methodName,
        ToIntFunction<ZonedDateTime> function
    ) {
        super(indexFieldData, multiValueMode);

        Objects.requireNonNull(methodName);

        this.methodName = methodName;
        this.function = function;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext leaf, DoubleValues scores) {
        LeafNumericFieldData leafData = (LeafNumericFieldData) fieldData.load(leaf);
        NumericDoubleValues docValues = multiValueMode.select(leafData.getDoubleValues());
        return new DoubleValues() {
            @Override
            public double doubleValue() throws IOException {
                return function.applyAsInt(ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) docValues.doubleValue()), ZoneOffset.UTC));
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return docValues.advanceExact(doc);
            }
        };
    }

    @Override
    public String toString() {
        return methodName + ": field(" + fieldData.getFieldName() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;

        DateObjectValueSource that = (DateObjectValueSource) o;
        return methodName.equals(that.methodName);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + methodName.hashCode();
        return result;
    }
}
