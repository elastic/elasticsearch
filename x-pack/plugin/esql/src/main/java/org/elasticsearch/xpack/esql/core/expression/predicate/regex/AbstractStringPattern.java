/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public abstract class AbstractStringPattern implements StringPattern {

    private static final Logger logger = LogManager.getLogger(AbstractStringPattern.class);

    private Automaton automaton;

    public final Automaton createAutomaton(boolean ignoreCase) {
        try {
            return doCreateAutomaton(ignoreCase);
        } catch (TooComplexToDeterminizeException e) {
            throw new IllegalArgumentException("Pattern was too complex to determinize", e);
        }
    }

    protected abstract Automaton doCreateAutomaton(boolean ignoreCase);

    private Automaton automaton() {
        if (automaton == null) {
            automaton = createAutomaton(false);
        }
        return automaton;
    }

    @Override
    public boolean matchesAll() {
        return Operations.isTotal(automaton());
    }

    @Override
    public String exactMatch() {
        Automaton a = automaton();
        if (a.getNumStates() == 0) { // workaround for https://github.com/elastic/elasticsearch/pull/128887
            return null; // Empty automaton has no matches
        }
        IntsRef singleton = Operations.getSingleton(a);
        return singleton != null ? UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length) : null;
    }

    /**
     * Returns the longest string that prefixes every string accepted by this pattern,
     * using Lucene's {@link Operations#getCommonPrefix}. Returns {@code null} if the
     * common prefix is empty (e.g. the pattern starts with a wildcard).
     */
    public String extractPrefix() {
        Automaton a = automaton();
        if (a.getNumStates() == 0) {
            return null;
        }
        Automaton clean = Operations.removeDeadStates(a);
        if (clean.getNumStates() == 0) {
            return null;
        }
        String prefix = Operations.getCommonPrefix(clean);
        return prefix.isEmpty() ? null : prefix;
    }

    /**
     * Returns the longest string that suffixes every string accepted by this pattern.
     * Computed by reversing the automaton and extracting the common prefix of the reversed
     * language, then reversing the result. Returns {@code null} if the common suffix is empty
     * or the reversed automaton is too complex to determinize.
     */
    public String extractSuffix() {
        Automaton a = automaton();
        if (a.getNumStates() == 0) {
            return null;
        }
        Automaton reversed;
        try {
            reversed = Operations.determinize(Operations.reverse(a), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        } catch (TooComplexToDeterminizeException e) {
            logger.debug("reversed automaton too complex to determinize for suffix extraction", e);
            return null;
        }
        reversed = Operations.removeDeadStates(reversed);
        if (reversed.getNumStates() == 0) {
            return null;
        }
        String reversedPrefix = Operations.getCommonPrefix(reversed);
        if (reversedPrefix.isEmpty()) {
            return null;
        }
        int[] codePoints = reversedPrefix.codePoints().toArray();
        StringBuilder sb = new StringBuilder();
        for (int i = codePoints.length - 1; i >= 0; i--) {
            sb.appendCodePoint(codePoints[i]);
        }
        return sb.toString();
    }
}
