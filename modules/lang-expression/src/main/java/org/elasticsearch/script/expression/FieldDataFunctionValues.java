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

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;

/**
 * A {@link org.apache.lucene.queries.function.FunctionValues} which wrap field data.
 */
class FieldDataFunctionValues extends DoubleDocValues {
    NumericDoubleValues dataAccessor;

    FieldDataFunctionValues(ValueSource parent, MultiValueMode m, AtomicNumericFieldData d) {
        super(parent);
        dataAccessor = m.select(d.getDoubleValues(), 0d);
    }

    @Override
    public double doubleVal(int i) {
        return dataAccessor.get(i);
    }
}
