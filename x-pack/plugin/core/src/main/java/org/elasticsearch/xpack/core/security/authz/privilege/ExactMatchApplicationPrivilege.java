/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;

class ExactMatchApplicationPrivilege extends ApplicationPrivilege {

    private final StringMatcher exactMatcher;
    private Automaton automaton;

    ExactMatchApplicationPrivilege(String application, Set<String> name, String... patterns) {
        super(application, name, patterns);
        this.exactMatcher = StringMatcher.of(patterns);
    }

    @Override
    public boolean patternsTotal() {
        return exactMatcher.isTotal();
    }

    @Override
    public boolean patternsEmpty() {
        return exactMatcher.isEmpty();
    }

    @Override
    public boolean supersetOfPatterns(ApplicationPrivilege other) {
        for (var pattern : other.getPatterns()) {
            if (false == exactMatcher.test(pattern)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Automaton getAutomaton() {
        if (automaton == null) {
            automaton = patterns(getPatterns());
        }
        return automaton;
    }
}
