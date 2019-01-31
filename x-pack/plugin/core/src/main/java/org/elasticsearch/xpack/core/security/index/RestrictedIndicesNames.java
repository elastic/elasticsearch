/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.index;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.upgrade.IndexUpgradeCheckVersion;

import java.util.Collections;
import java.util.Set;

public final class RestrictedIndicesNames {
    public static final String AUDIT_INDEX_NAME_PREFIX = ".security_audit_log";
    public static final String INTERNAL_SECURITY_INDEX = ".security-" + IndexUpgradeCheckVersion.UPRADE_VERSION;
    public static final String SECURITY_INDEX_NAME = ".security";

    public static final Set<String> NAMES_SET = Collections.unmodifiableSet(Sets.newHashSet(SECURITY_INDEX_NAME, INTERNAL_SECURITY_INDEX));
    public static final Automaton NAMES_AUTOMATON = Automatons.patterns(NAMES_SET);

    private RestrictedIndicesNames() {
    }
}