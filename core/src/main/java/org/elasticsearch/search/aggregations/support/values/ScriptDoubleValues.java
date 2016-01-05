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

import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link SortingNumericDoubleValues} implementation which is based on a script
 */
public class ScriptDoubleValues extends SortingNumericDoubleValues implements ScorerAware {

    final LeafSearchScript script;

    Map extendedScriptResult;

    public ScriptDoubleValues(LeafSearchScript script) {
        super();
        this.script = script;
    }

    @Override
    public void setDocument(int docId) {
        script.setDocument(docId);
        final Object value = script.run();

        if (value == null) {
            resize(0);
        }

        else if (value instanceof Number) {
            resize(1);
            values[0] = ((Number) value).doubleValue();
        }

        else if (value.getClass().isArray()) {
            resize(Array.getLength(value));
            for (int i = 0; i < count(); ++i) {
                values[i] = ((Number) Array.get(value, i)).doubleValue();
            }
        }

        else if (value instanceof Collection) {
            resize(((Collection<?>) value).size());
            int i = 0;
            for (Iterator<?> it = ((Collection<?>) value).iterator(); it.hasNext(); ++i) {
                values[i] = ((Number) it.next()).doubleValue();
            }
            assert i == count();
        } else if(value instanceof Map) {
            // Map containing one or several values + some optional extended properties.
            // Aggregators can use these extended properties to implement specific behaviors (eg: AvgAggregator using
            // the property weight(s) to compute a weighted average instead of standard average).
            extendedScriptResult = (Map)value;
            Object scriptValue = extendedScriptResult.remove("value");
            Object scriptValues = extendedScriptResult.remove("values");
            if(scriptValues != null && scriptValues instanceof Collection) {
                resize(((Collection)scriptValues).size());
                int i = 0;
                for (Iterator<?> it = ((Collection<?>) scriptValues).iterator(); it.hasNext(); ++i) {
                    values[i] = ((Number) it.next()).doubleValue();
                }
                assert i == count();
            } else if(scriptValue != null && scriptValue instanceof Number) {
                resize(1);
                values[0] = ((Number)scriptValue).doubleValue();
            } else {
                throw new AggregationExecutionException("Unsupported script value [" + value + "]");
            }
        }

        else {
            throw new AggregationExecutionException("Unsupported script value [" + value + "]");
        }

        sort();
    }

    public Map getExtendedScriptResult() {
        return extendedScriptResult;
    }

    public final boolean hasExtendedScriptResult() {
        return extendedScriptResult != null && !extendedScriptResult.isEmpty();
    }

    @Override
    public void setScorer(Scorer scorer) {
        script.setScorer(scorer);
    }
}
