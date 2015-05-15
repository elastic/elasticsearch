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
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

/**
 * FunctionValues to get the count of the number of values in a field for a document.
 */
public class CountMethodFunctionValues extends DoubleDocValues {
    SortedNumericDoubleValues values;

    CountMethodFunctionValues(ValueSource parent, AtomicNumericFieldData fieldData) {
        super(parent);

        values = fieldData.getDoubleValues();
    }

    @Override
    public double doubleVal(int doc) {
        values.setDocument(doc);
        return values.count();
    }
}
