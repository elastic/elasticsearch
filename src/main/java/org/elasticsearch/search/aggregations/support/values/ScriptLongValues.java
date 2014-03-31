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

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.ScriptValues;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * {@link LongValues} implementation which is based on a script
 */
public class ScriptLongValues extends LongValues implements ScriptValues {

    final SearchScript script;

    private Object value;
    private long[] values = new long[1];
    private int valueCount;
    private int valueOffset;

    public ScriptLongValues(SearchScript script) {
        super(true); // assume multi-valued
        this.script = script;
    }

    @Override
    public SearchScript script() {
        return script;
    }

    @Override
    public int setDocument(int docId) {
        this.docId = docId;
        script.setNextDocId(docId);
        value = script.run();

        if (value == null) {
            valueCount = 0;
        }

        else if (value instanceof Number) {
            valueCount = 1;
            values[0] = ((Number) value).longValue();
        }

        else if (value.getClass().isArray()) {
            valueCount = Array.getLength(value);
            values = ArrayUtil.grow(values, valueCount);
            for (int i = 0; i < valueCount; ++i) {
                values[i] = ((Number) Array.get(value, i)).longValue();
            }
        }

        else if (value instanceof Collection) {
            valueCount = ((Collection<?>) value).size();
            values = ArrayUtil.grow(values, valueCount);
            int i = 0;
            for (Iterator<?> it = ((Collection<?>) value).iterator(); it.hasNext(); ++i) {
                values[i] = ((Number) it.next()).longValue();
            }
            assert i == valueCount;
        }

        else {
            throw new AggregationExecutionException("Unsupported script value [" + value + "]");
        }

        valueOffset = 0;
        return valueCount;
    }

    @Override
    public long nextValue() {
        assert valueOffset < valueCount;
        return values[valueOffset++];
    }

}
