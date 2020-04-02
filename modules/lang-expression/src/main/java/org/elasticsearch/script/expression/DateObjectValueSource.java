/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableDateTime;

import java.io.IOException;
import java.util.Objects;
import java.util.function.ToIntFunction;

/** Extracts a portion of a date field with joda time */
class DateObjectValueSource extends FieldDataValueSource {

    final String methodName;
    final ToIntFunction<ReadableDateTime> function;

    DateObjectValueSource(IndexFieldData<?> indexFieldData, MultiValueMode multiValueMode,
                          String methodName, ToIntFunction<ReadableDateTime> function) {
        super(indexFieldData, multiValueMode);

        Objects.requireNonNull(methodName);

        this.methodName = methodName;
        this.function = function;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext leaf, DoubleValues scores) {
        LeafNumericFieldData leafData = (LeafNumericFieldData) fieldData.load(leaf);
        MutableDateTime joda = new MutableDateTime(0, DateTimeZone.UTC);
        NumericDoubleValues docValues = multiValueMode.select(leafData.getDoubleValues());
        return new DoubleValues() {
            @Override
            public double doubleValue() throws IOException {
                joda.setMillis((long)docValues.doubleValue());
                return function.applyAsInt(joda);
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
        if (!super.equals(o)) return false;

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
