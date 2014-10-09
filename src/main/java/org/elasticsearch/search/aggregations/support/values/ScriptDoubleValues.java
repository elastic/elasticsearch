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
package org.elasticsearch.search.aggregations.support.values;

import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.ScriptValues;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * {@link DoubleValues} implementation which is based on a script
 */
public class ScriptDoubleValues extends SortingNumericDoubleValues implements ScriptValues {

    final SearchScript script;

    public ScriptDoubleValues(SearchScript script) {
        super();
        this.script = script;
    }

    @Override
    public SearchScript script() {
        return script;
    }

    @Override
    public void setDocument(int docId) {
        script.setNextDocId(docId);
        final Object value = script.run();

        if (value == null) {
            count = 0;
        }

        else if (value instanceof Number) {
            count = 1;
            values[0] = ((Number) value).doubleValue();
        }

        else if (value.getClass().isArray()) {
            count = Array.getLength(value);
            grow();
            for (int i = 0; i < count; ++i) {
                values[i] = ((Number) Array.get(value, i)).doubleValue();
            }
        }

        else if (value instanceof Collection) {
            count = ((Collection<?>) value).size();
            grow();
            int i = 0;
            for (Iterator<?> it = ((Collection<?>) value).iterator(); it.hasNext(); ++i) {
                values[i] = ((Number) it.next()).doubleValue();
            }
            assert i == count;
        }

        else {
            throw new AggregationExecutionException("Unsupported script value [" + value + "]");
        }

        sort();
    }
}
