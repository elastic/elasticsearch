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
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

/** Extracts a portion of a date field with {@code Calendar.get()} */
class DateMethodValueSource extends FieldDataValueSource {

    final String methodName;
    final int calendarType;

    DateMethodValueSource(IndexFieldData<?> indexFieldData, MultiValueMode multiValueMode, String methodName, int calendarType) {
        super(indexFieldData, multiValueMode);

        Objects.requireNonNull(methodName);

        this.methodName = methodName;
        this.calendarType = calendarType;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext leaf, DoubleValues scores) {
        LeafNumericFieldData leafData = (LeafNumericFieldData) fieldData.load(leaf);
        final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        NumericDoubleValues docValues = multiValueMode.select(leafData.getDoubleValues());
        return new DoubleValues() {
            @Override
            public double doubleValue() throws IOException {
                calendar.setTimeInMillis((long)docValues.doubleValue());
                return calendar.get(calendarType);
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

        DateMethodValueSource that = (DateMethodValueSource) o;

        if (calendarType != that.calendarType) return false;
        return methodName.equals(that.methodName);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + methodName.hashCode();
        result = 31 * result + calendarType;
        return result;
    }
}
