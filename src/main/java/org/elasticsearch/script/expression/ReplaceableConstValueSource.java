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
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link ValueSource} which has a stub {@link FunctionValues} that holds a dynamically replaceable constant double.
 */
class ReplaceableConstValueSource extends ValueSource {
    double value;
    final FunctionValues fv;

    public ReplaceableConstValueSource() {
        fv = new DoubleDocValues(this) {
            @Override
            public double doubleVal(int i) {
                return value;
            }
        };
    }

    @Override
    public FunctionValues getValues(Map map, LeafReaderContext atomicReaderContext) throws IOException {
        return fv;
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public String description() {
        return "replaceableConstDouble";
    }

    public void setValue(double v) {
        value = v;
    }
}
