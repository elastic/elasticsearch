/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.index;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.List;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

public final class RestrictedIndicesNames {
    public static final String INTERNAL_SECURITY_MAIN_INDEX_6 = ".security-6";
    public static final String INTERNAL_SECURITY_MAIN_INDEX_7 = ".security-7";
    public static final String SECURITY_MAIN_ALIAS = ".security";

    public static final String INTERNAL_SECURITY_TOKENS_INDEX_7 = ".security-tokens-7";
    public static final String SECURITY_TOKENS_ALIAS = ".security-tokens";

    // See o.e.x.security.Security#getSecurityMainIndexDescriptor, o.e.x.security.Security#getSecurityTokensIndexDescriptor
    private static final Automaton SECURITY_INDEX_AUTOMATON = Operations.concatenate(
        List.of(
            Operations.union(Automata.makeString(SECURITY_MAIN_ALIAS), Automata.makeString(SECURITY_TOKENS_ALIAS)),
            Automata.makeChar('-'),
            Automata.makeCharRange('0', '9'),
            Automata.makeAnyString()
        )
    );
    private static final Predicate<String> SECURITY_INDEX_PREDICATE = Automatons.predicate(SECURITY_INDEX_AUTOMATON);

    // public for tests
    public static final String ASYNC_SEARCH_PREFIX = ".async-search";
    private static final Automaton ASYNC_SEARCH_AUTOMATON = Automatons.patterns(ASYNC_SEARCH_PREFIX + "*");

    // public for tests
    public static final Set<String> RESTRICTED_NAMES = Collections.unmodifiableSet(
        Sets.newHashSet(
            SECURITY_MAIN_ALIAS,
            INTERNAL_SECURITY_MAIN_INDEX_6,
            INTERNAL_SECURITY_MAIN_INDEX_7,
            INTERNAL_SECURITY_TOKENS_INDEX_7,
            SECURITY_TOKENS_ALIAS
        )
    );

    public static boolean isRestricted(String concreteIndexName) {
        return RESTRICTED_NAMES.contains(concreteIndexName)
            || concreteIndexName.startsWith(ASYNC_SEARCH_PREFIX)
            || SECURITY_INDEX_PREDICATE.test(concreteIndexName);
    }

    public static final Automaton NAMES_AUTOMATON = Automatons.unionAndMinimize(
        Arrays.asList(Automatons.patterns(RESTRICTED_NAMES), ASYNC_SEARCH_AUTOMATON, SECURITY_INDEX_AUTOMATON)
    );

    private RestrictedIndicesNames() {}
}
