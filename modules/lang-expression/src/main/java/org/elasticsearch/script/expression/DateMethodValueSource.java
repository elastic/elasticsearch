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
import org.apache.lucene.queries.function.FunctionValues;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

class DateMethodValueSource extends FieldDataValueSource {

    protected final String methodName;
    protected final int calendarType;

    DateMethodValueSource(IndexFieldData<?> indexFieldData, MultiValueMode multiValueMode, String methodName, int calendarType) {
        super(indexFieldData, multiValueMode);

        Objects.requireNonNull(methodName);

        this.methodName = methodName;
        this.calendarType = calendarType;
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext leaf) throws IOException {
        AtomicFieldData leafData = fieldData.load(leaf);
        assert(leafData instanceof AtomicNumericFieldData);

        return new DateMethodFunctionValues(this, multiValueMode, (AtomicNumericFieldData)leafData, calendarType);
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
