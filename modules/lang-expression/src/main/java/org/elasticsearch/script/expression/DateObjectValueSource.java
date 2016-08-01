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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableDateTime;

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
    @SuppressWarnings("rawtypes") // ValueSource uses a rawtype
    public FunctionValues getValues(Map context, LeafReaderContext leaf) throws IOException {
        AtomicNumericFieldData leafData = (AtomicNumericFieldData) fieldData.load(leaf);
        MutableDateTime joda = new MutableDateTime(0, DateTimeZone.UTC);
        NumericDoubleValues docValues = multiValueMode.select(leafData.getDoubleValues(), 0d);
        return new DoubleDocValues(this) {
          @Override
          public double doubleVal(int docId) {
            long millis = (long)docValues.get(docId);
            joda.setMillis(millis);
            return function.applyAsInt(joda);
          }
        };
    }

    @Override
    public String description() {
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
