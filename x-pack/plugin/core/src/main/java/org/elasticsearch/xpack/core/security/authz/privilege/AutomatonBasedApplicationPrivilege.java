/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;

public class AutomatonBasedApplicationPrivilege extends ApplicationPrivilege {
    private final Automaton automaton;

    AutomatonBasedApplicationPrivilege(String application, Set<String> name, String... patterns) {
        super(application, name, patterns);
        this.automaton = patterns(patterns);
    }

    @Override
    public boolean patternsTotal() {
        return Operations.isTotal(automaton);
    }

    @Override
    public boolean patternsEmpty() {
        return Operations.isEmpty(automaton);
    }

    @Override
    public boolean supersetOfPatterns(ApplicationPrivilege other) {
        final String[] patterns = other.getPatterns();
        final Automaton otherAutomaton = other instanceof AutomatonBasedApplicationPrivilege def ? def.automaton : patterns(patterns);
        return Operations.subsetOf(otherAutomaton, automaton);
    }

    @Override
    public boolean supersetOfPatterns(String... patterns) {
        return Operations.subsetOf(patterns(patterns), automaton);
    }
}
