/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A permission that is based on privileges for application (non elasticsearch) capabilities
 */
public final class ApplicationPermission {

    public static final ApplicationPermission NONE = new ApplicationPermission(Collections.emptyList());

    private final Logger logger;
    private final Map<ApplicationPrivilege, Automaton> privileges;

    public ApplicationPermission(List<Tuple<ApplicationPrivilege, Set<String>>> tuples) {
        this.logger = Loggers.getLogger(getClass());
        this.privileges = new HashMap<>();
        tuples.forEach(tup -> privileges.compute(tup.v1(), (k, existing) -> {
            final Automaton patterns = Automatons.patterns(tup.v2());
            if (existing == null) {
                return patterns;
            } else {
                return Automatons.unionAndMinimize(Arrays.asList(existing, patterns));
            }
        }));
    }

    public boolean grants(ApplicationPrivilege other, String resource) {
        Automaton resourceAutomaton = Automatons.patterns(resource);
        final boolean matched = privileges.entrySet().stream()
            .anyMatch(entry -> Objects.equals(other.getApplication(), entry.getKey().getApplication())
                && Operations.subsetOf(other.getAutomaton(), entry.getKey().getAutomaton())
                && Operations.subsetOf(resourceAutomaton, entry.getValue()));
        logger.debug("Permission [{}] {} grant [{} , {}]", this, matched ? "does" : "does not", other, resource);
        return matched;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{privileges=" + privileges.keySet() + "}";
    }
}
