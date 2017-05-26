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

package org.elasticsearch.painless;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * ScriptImpl can be used as either an {@link ExecutableScript} or a {@link SearchScript}
 * to run a previously compiled Painless script.
 */
final class ScriptImpl extends SearchScript {

    /**
     * The Painless script that can be run.
     */
    private final GenericElasticsearchScript script;

    /**
     * A map that can be used to access input parameters at run-time.
     */
    private final Map<String, Object> variables;

    /**
     * Looks up the {@code ctx} from the {@link #variables} if {@code ctx} is used, otherwise return {@code null}.
     */
    private final Function<Map<String, Object>, Map<?, ?>> ctxLookup;

    /**
     * Current _value for aggregation
     * @see #setNextAggregationValue(Object)
     */
    private Object aggregationValue;

    /**
     * Creates a ScriptImpl for the a previously compiled Painless script.
     * @param script The previously compiled Painless script.
     * @param vars The initial variables to run the script with.
     * @param lookup The lookup to allow search fields to be available if this is run as a search script.
     */
    ScriptImpl(final GenericElasticsearchScript script, final Map<String, Object> vars, final SearchLookup lookup) {
        super(vars, lookup);
        this.script = script;
        this.variables = new HashMap<>();

        if (vars != null) {
            variables.putAll(vars);
        }

        ctxLookup = script.uses$ctx() ? variables -> (Map<?, ?>) variables.get("ctx") : variables -> null;
    }

    @Override
    public SearchScript forSegment(LeafReaderContext leaf) {
        ScriptImpl script = (ScriptImpl) super.forSegment(leaf);
        LeafSearchLookup leafLookup = script.getLeafLookup();
        if (leafLookup != null) {
            script.variables.putAll(leafLookup.asMap());
        }
        return script;
    }

    @Override
    public void setNextVar(final String name, final Object value) {
        variables.put(name, value);
    }

    @Override
    public void setNextAggregationValue(Object value) {
        this.aggregationValue = value;
    }

    @Override
    public Object run() {
        return script.execute(variables, getScore(), getDoc(), aggregationValue, ctxLookup.apply(variables));
    }

    @Override
    public double runAsDouble() {
        return ((Number)run()).doubleValue();
    }

    @Override
    public long runAsLong() {
        return ((Number)run()).longValue();
    }

    @Override
    public boolean needsScores() {
        return script.uses$_score();
    }
}
