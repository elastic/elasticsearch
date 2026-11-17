/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;

import java.util.List;

/** Compiles the wildcard patterns of the tracing settings, in which {@code *} matches any sequence of characters. */
final class PatternAutomata {

    private PatternAutomata() {}

    /**
     * Matches what is matched by any of the {@code includePatterns} (anything if there are none), unless it is also matched by any of the
     * {@code excludePatterns}.
     */
    static CharacterRunAutomaton compile(List<String> includePatterns, List<String> excludePatterns) {
        Automaton includeAutomaton = patternsToAutomaton(includePatterns);
        Automaton excludeAutomaton = patternsToAutomaton(excludePatterns);

        if (includeAutomaton == null) {
            includeAutomaton = Automata.makeAnyString();
        }

        final Automaton finalAutomaton = excludeAutomaton == null
            ? includeAutomaton
            : Operations.minus(includeAutomaton, excludeAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);

        return new CharacterRunAutomaton(MinimizationOperations.minimize(finalAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }

    private static Automaton patternsToAutomaton(List<String> patterns) {
        final List<Automaton> automata = patterns.stream().map(s -> {
            final String regex = s.replace(".", "\\.").replace("*", ".*");
            return new RegExp(regex, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT).toAutomaton();
        }).toList();
        if (automata.isEmpty()) {
            return null;
        }
        if (automata.size() == 1) {
            return automata.get(0);
        }
        return Operations.union(automata);
    }
}
