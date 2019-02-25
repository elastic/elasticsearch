/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.index;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.Set;

public final class RestrictedIndicesNames {
    public static final String INTERNAL_SECURITY_INDEX_6 = ".security-6";
    public static final String INTERNAL_SECURITY_INDEX_7 = ".security-7";
    public static final String SECURITY_INDEX_NAME = ".security";

    /**
     * A set of names for the security indices
     */
    public static final Set<String> SECURITY_INDICES = Collections.unmodifiableSet(
        Sets.newHashSet(SECURITY_INDEX_NAME, INTERNAL_SECURITY_INDEX_6, INTERNAL_SECURITY_INDEX_7));

    /**
     * All restricted indices (currently only security indices, but there could be more in the future)
     */
    public static final Set<String> NAMES_SET = SECURITY_INDICES;
    public static final Automaton NAMES_AUTOMATON = Automatons.patterns(NAMES_SET);

    private RestrictedIndicesNames() {
    }
}
