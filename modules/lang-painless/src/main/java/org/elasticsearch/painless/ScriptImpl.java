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
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;

/**
 * ScriptImpl can be used as either an {@link ExecutableScript} or a {@link LeafSearchScript}
 * to run a previously compiled Painless script.
 */
final class ScriptImpl implements ExecutableScript, LeafSearchScript {

    /**
     * The Painless Executable script that can be run.
     */
    private final Executable executable;

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
     * @param executable The previously compiled Painless script.
     * @param vars The initial variables to run the script with.
     * @param lookup The lookup to allow search fields to be available if this is run as a search script.
     */
    ScriptImpl(final Executable executable, final Map<String, Object> vars, final LeafSearchLookup lookup) {
        this.executable = executable;
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
        try {
            return executable.execute(variables, scorer, doc, aggregationValue);
        // Note that it is safe to catch any of the following errors since Painless is stateless.
        } catch (Debug.PainlessExplainError e) {
            throw convertToScriptException(e, e.getHeaders());
        } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
            throw convertToScriptException(e, emptyMap());
        }
    }

    /**
     * Adds stack trace and other useful information to exceptions thrown
     * from a Painless script.
     * @param t The throwable to build an exception around.
     * @return The generated ScriptException.
     */
    private ScriptException convertToScriptException(Throwable t, Map<String, List<String>> headers) {
        IntUnaryOperator calculateStartOffset = offset -> {
            int startOffset = executable.getPreviousStatement(offset);
            if (startOffset == -1) {
                assert false; // should never happen unless we hit exc in ctor prologue...
                return 0;
            }
            return startOffset;
        };
        IntUnaryOperator calculateEndOffset = offset -> {
            int endOffset = executable.getNextStatement(offset);
            if (endOffset == -1) {
                return executable.getSource().length();
            }
            return endOffset;
        };
        Predicate<StackTraceElement> shouldFilter = element ->
                   element.getClassName().startsWith("org.elasticsearch.painless.")
                || element.getClassName().startsWith("java.lang.invoke.")
                || element.getClassName().startsWith("sun.invoke.");
        ScriptException scriptException = PainlessScriptEngineService.convertToScriptException("runtime error", executable.getName(),
                executable.getSource(), t, calculateStartOffset, calculateEndOffset, shouldFilter);
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            scriptException.addHeader(header.getKey(), header.getValue());
        }
        return scriptException;
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
}
