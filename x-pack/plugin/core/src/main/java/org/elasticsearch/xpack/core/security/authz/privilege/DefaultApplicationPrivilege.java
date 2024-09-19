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

public class DefaultApplicationPrivilege extends ApplicationPrivilege {
    private final Automaton automaton;

    DefaultApplicationPrivilege(String application, Set<String> name, String... patterns) {
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
        return Operations.subsetOf(other.getAutomaton(), automaton);
    }

    @Override
    public Automaton getAutomaton() {
        return automaton;
    }
}
