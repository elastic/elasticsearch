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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A permission that is based on privileges for application (non elasticsearch) capabilities
 */
public final class ApplicationPermission {

    public static final ApplicationPermission NONE = new ApplicationPermission(Collections.emptyList());

    private final Logger logger;
    private final List<PermissionEntry> permissions;

    public ApplicationPermission(List<Tuple<ApplicationPrivilege, Set<String>>> tuples) {
        this.logger = Loggers.getLogger(getClass());
        Map<ApplicationPrivilege, PermissionEntry> permissionsByPrivilege = new HashMap<>();
        tuples.forEach(tup -> permissionsByPrivilege.compute(tup.v1(), (k, existing) -> {
            final Automaton patterns = Automatons.patterns(tup.v2());
            if (existing == null) {
                return new PermissionEntry(k, patterns);
            } else {
                return new PermissionEntry(k, Automatons.unionAndMinimize(Arrays.asList(existing.resources, patterns)));
            }
        }));
        this.permissions = new ArrayList<>(permissionsByPrivilege.values());
    }

    public boolean grants(ApplicationPrivilege other, String resource) {
        Automaton resourceAutomaton = Automatons.patterns(resource);
        final boolean matched = permissions.stream().anyMatch(e -> e.grants(other, resourceAutomaton));
        logger.debug("Permission [{}] {} grant [{} , {}]", this, matched ? "does" : "does not", other, resource);
        return matched;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{privileges=" + permissions + "}";
    }

    private static class PermissionEntry {
        private final ApplicationPrivilege privilege;
        private final Predicate<String> application;
        private final Automaton resources;

        private PermissionEntry(ApplicationPrivilege privilege, Automaton resources) {
            this.privilege = privilege;
            this.application = Automatons.predicate(privilege.getApplication());
            this.resources = resources;
        }

        public boolean grants(ApplicationPrivilege other, Automaton resource) {
            return this.application.test(other.getApplication())
                && Operations.subsetOf(other.getAutomaton(), privilege.getAutomaton())
                && Operations.subsetOf(resource, this.resources);
        }

        @Override
        public String toString() {
            return privilege.toString();
        }
    }
}
