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

import org.apache.lucene.search.Scorable;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.script.AggregationScript;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;

/**
 * {@link SortedBinaryDocValues} implementation that reads values from a script.
 */
public class ScriptBytesValues extends SortingBinaryDocValues implements ScorerAware {

    private final AggregationScript script;

    public ScriptBytesValues(AggregationScript script) {
        super();
        this.script = script;
    }

    private void set(int i, Object o) {
        if (o == null) {
            values[i].clear();
        } else {
            CollectionUtils.ensureNoSelfReferences(o, "ScriptBytesValues value");
            values[i].copyChars(o.toString());
        }
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        script.setDocument(doc);
        final Object value = script.execute();
        boolean isValueValid = value != null;

        if (isValueValid) {
            boolean valueIsArray = value.getClass().isArray();
            boolean valueIsCollection = value instanceof Collection;   
            int count = valueIsArray ? Array.getLength(value) : valueIsCollection ? ((Collection<?>) value).size() : 0 ;

            if (count > 0) {
                grow();
                if (isArray) {
                    for (int i = 0; i < count; ++i) {
                        set(i, Array.get(value, i));
                    }
                } else {
                    final Collection<?> coll = (Collection<?>) value;
                    int i = 0;
                    for (Object v : coll) {
                        set(i++, v);
                    }
                }
            } else {
                set(count, value);
            }
            sort();
        }

        return isValueValid;
    }

    @Override
    public void setScorer(Scorable scorer) {
        script.setScorer(scorer);
    }
}
