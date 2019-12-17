/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.index;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public final class RestrictedIndicesNames {
    public static final String INTERNAL_SECURITY_MAIN_INDEX_6 = ".security-6";
    public static final String INTERNAL_SECURITY_MAIN_INDEX_7 = ".security-7";
    public static final String SECURITY_MAIN_ALIAS = ".security";

    public static final String INTERNAL_SECURITY_TOKENS_INDEX_7 = ".security-tokens-7";
    public static final String SECURITY_TOKENS_ALIAS = ".security-tokens";

    // public for tests
    public static final String ASYNC_SEARCH_PREFIX = ".async-search-";
    private static final CharacterRunAutomaton ASYNC_SEARCH_AUTOMATON =
            new CharacterRunAutomaton(Automatons.patterns(ASYNC_SEARCH_PREFIX + "*"));

    // public for tests
    public static final Set<String> RESTRICTED_NAMES = Collections.unmodifiableSet(Sets.newHashSet(SECURITY_MAIN_ALIAS,
            INTERNAL_SECURITY_MAIN_INDEX_6, INTERNAL_SECURITY_MAIN_INDEX_7, INTERNAL_SECURITY_TOKENS_INDEX_7, SECURITY_TOKENS_ALIAS));

    public static boolean isRestricted(String concreteIndexName) {
        return RESTRICTED_NAMES.contains(concreteIndexName) || ASYNC_SEARCH_AUTOMATON.run(concreteIndexName);
    }

    public static final Automaton NAMES_AUTOMATON = Automatons.unionAndMinimize(Arrays.asList(Automatons.patterns(RESTRICTED_NAMES),
            Automatons.patterns(ASYNC_SEARCH_PREFIX + "*")));

    private RestrictedIndicesNames() {
    }
}
