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

import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * ScriptImpl can be used as either an {@link ExecutableScript} or a {@link LeafSearchScript}
 * to run a previously compiled Painless script.
 */
final class ScriptImpl implements ExecutableScript, LeafSearchScript {

    /**
     * The Painless script that can be run.
     */
    private final GenericElasticsearchScript script;

    /**
     * A map that can be used to access input parameters at run-time.
     */
    private final Map<String, Object> variables;

    /**
     * The lookup is used to access search field values at run-time.
     */
    private final LeafSearchLookup lookup;

    /**
     * the 'doc' object accessed by the script, if available.
     */
    private final LeafDocLookup doc;

    /**
     * Looks up the {@code _score} from {@link #scorer} if {@code _score} is used, otherwise returns {@code 0.0}.
     */
    private final ScoreLookup scoreLookup;

    /**
     * Looks up the {@code ctx} from the {@link #variables} if {@code ctx} is used, otherwise return {@code null}.
     */
    private final Function<Map<String, Object>, Map<?, ?>> ctxLookup;

    /**
     * Current scorer being used
     * @see #setScorer(Scorer)
     */
    private Scorer scorer;

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
    ScriptImpl(final GenericElasticsearchScript script, final Map<String, Object> vars, final LeafSearchLookup lookup) {
        this.script = script;
        this.lookup = lookup;
        this.variables = new HashMap<>();

        if (vars != null) {
            variables.putAll(vars);
        }

        if (lookup != null) {
            variables.putAll(lookup.asMap());
            doc = lookup.doc();
        } else {
            doc = null;
        }

        scoreLookup = script.uses$_score() ? ScriptImpl::getScore : scorer -> 0.0;
        ctxLookup = script.uses$ctx() ? variables -> (Map<?, ?>) variables.get("ctx") : variables -> null;
    }

    /**
     * Set a variable for the script to be run against.
     * @param name The variable name.
     * @param value The variable value.
     */
    @Override
    public void setNextVar(final String name, final Object value) {
        variables.put(name, value);
    }

    /**
     * Set the next aggregation value.
     * @param value Per-document value, typically a String, Long, or Double.
     */
    @Override
    public void setNextAggregationValue(Object value) {
        this.aggregationValue = value;
    }

    /**
     * Run the script.
     * @return The script result.
     */
    @Override
    public Object run() {
        return script.execute(variables, scoreLookup.apply(scorer), doc, aggregationValue, ctxLookup.apply(variables));
    }

    /**
     * Run the script.
     * @return The script result as a double.
     */
    @Override
    public double runAsDouble() {
        return ((Number)run()).doubleValue();
    }

    /**
     * Run the script.
     * @return The script result as a long.
     */
    @Override
    public long runAsLong() {
        return ((Number)run()).longValue();
    }

    /**
     * Sets the scorer to be accessible within a script.
     * @param scorer The scorer used for a search.
     */
    @Override
    public void setScorer(final Scorer scorer) {
        this.scorer = scorer;
    }

    /**
     * Sets the current document.
     * @param doc The current document.
     */
    @Override
    public void setDocument(final int doc) {
        if (lookup != null) {
            lookup.setDocument(doc);
        }
    }

    /**
     * Sets the current source.
     * @param source The current source.
     */
    @Override
    public void setSource(final Map<String, Object> source) {
        if (lookup != null) {
            lookup.source().setSource(source);
        }
    }

    private static double getScore(Scorer scorer) {
        try {
            return scorer.score();
        } catch (IOException e) {
            throw new ElasticsearchException("couldn't lookup score", e);
        }
    }

    interface ScoreLookup {
        double apply(Scorer scorer);
    }
}
