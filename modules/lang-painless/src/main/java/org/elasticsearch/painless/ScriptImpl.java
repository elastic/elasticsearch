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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        } catch (PainlessError | BootstrapMethodError | IllegalAccessError | Exception t) {
            throw convertToScriptException(t);
        }
    }

    /**
     * Adds stack trace and other useful information to exceptiosn thrown
     * from a Painless script.
     * @param t The throwable to build an exception around.
     * @return The generated ScriptException.
     */
    private ScriptException convertToScriptException(Throwable t) {
        // create a script stack: this is just the script portion
        List<String> scriptStack = new ArrayList<>();
        for (StackTraceElement element : t.getStackTrace()) {
            if (WriterConstants.CLASS_NAME.equals(element.getClassName())) {
                // found the script portion
                int offset = element.getLineNumber();
                if (offset == -1) {
                    scriptStack.add("<<< unknown portion of script >>>");
                } else {
                    offset--; // offset is 1 based, line numbers must be!
                    int startOffset = executable.getPreviousStatement(offset);
                    if (startOffset == -1) {
                        assert false; // should never happen unless we hit exc in ctor prologue...
                        startOffset = 0;
                    }
                    int endOffset = executable.getNextStatement(startOffset);
                    if (endOffset == -1) {
                        endOffset = executable.getSource().length();
                    }
                    // TODO: if this is still too long, truncate and use ellipses
                    String snippet = executable.getSource().substring(startOffset, endOffset);
                    scriptStack.add(snippet);
                    StringBuilder pointer = new StringBuilder();
                    for (int i = startOffset; i < offset; i++) {
                        pointer.append(' ');
                    }
                    pointer.append("^---- HERE");
                    scriptStack.add(pointer.toString());
                }
                break;
            // but filter our own internal stacks (e.g. indy bootstrap)
            } else if (!shouldFilter(element)) {
                scriptStack.add(element.toString());
            }
        }
        // build a name for the script:
        final String name;
        if (PainlessScriptEngineService.INLINE_NAME.equals(executable.getName())) {
            name = executable.getSource();
        } else {
            name = executable.getName();
        }
        throw new ScriptException("runtime error", t, scriptStack, name, PainlessScriptEngineService.NAME);
    }

    /** returns true for methods that are part of the runtime */
    private static boolean shouldFilter(StackTraceElement element) {
        return element.getClassName().startsWith("org.elasticsearch.painless.") ||
               element.getClassName().startsWith("java.lang.invoke.") ||
               element.getClassName().startsWith("sun.invoke.");
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
