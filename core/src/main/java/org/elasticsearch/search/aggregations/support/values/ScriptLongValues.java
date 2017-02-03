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
import org.apache.lucene.util.LongValues;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.index.fielddata.SortingNumericDocValues;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.joda.time.ReadableInstant;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * {@link LongValues} implementation which is based on a script
 */
public class ScriptLongValues extends SortingNumericDocValues implements ScorerAware {

    final LeafSearchScript script;

    public ScriptLongValues(LeafSearchScript script) {
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

        else if (value.getClass().isArray()) {
            resize(Array.getLength(value));
            for (int i = 0; i < count(); ++i) {
                values[i] = toLongValue(Array.get(value, i));
            }
        }

        else if (value instanceof Collection) {
            resize(((Collection<?>) value).size());
            int i = 0;
            for (Iterator<?> it = ((Collection<?>) value).iterator(); it.hasNext(); ++i) {
                values[i] = toLongValue(it.next());
            }
            assert i == count();
        }

        else {
            resize(1);
            values[0] = toLongValue(value);
        }

        sort();
    }

    private static long toLongValue(Object o) {
        if (o instanceof Number) {
            return ((Number) o).longValue();
        } else if (o instanceof ReadableInstant) {
            // Dates are exposed in scripts as ReadableDateTimes but aggregations want them to be numeric
            return ((ReadableInstant) o).getMillis();
        } else if (o instanceof Boolean) {
            // We do expose boolean fields as boolean in scripts, however aggregations still expect
            // that scripts return the same internal representation as regular fields, so boolean
            // values in scripts need to be converted to a number, and the value formatter will
            // make sure of using true/false in the key_as_string field
            return ((Boolean) o).booleanValue() ? 1L : 0L;
        } else {
            throw new AggregationExecutionException("Unsupported script value [" + o + "], expected a number, date, or boolean");
        }
    }

    @Override
    public void setScorer(Scorer scorer) {
        script.setScorer(scorer);
    }
}
