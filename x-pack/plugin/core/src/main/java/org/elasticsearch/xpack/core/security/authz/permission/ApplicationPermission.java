/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A permission that is based on privileges for application (non elasticsearch) capabilities
 */
public final class ApplicationPermission {

    public static final ApplicationPermission NONE = new ApplicationPermission(Collections.emptyList());

    private static final Logger logger = LogManager.getLogger(ApplicationPermission.class);
    private final List<PermissionEntry> permissions;

    /**
     * @param privilegesAndResources A list of (privilege, resources). Each element in the {@link List} is a {@link Tuple} containing
     *                               a single {@link ApplicationPrivilege} and the {@link Set} of resources to which that privilege is
     *                               applied. The resources are treated as a wildcard {@link Automatons#pattern}.
     */
    ApplicationPermission(List<Tuple<ApplicationPrivilege, Set<String>>> privilegesAndResources) {
        Map<ApplicationPrivilege, PermissionEntry> permissionsByPrivilege = new HashMap<>();
        privilegesAndResources.forEach(tup -> permissionsByPrivilege.compute(tup.v1(), (appPriv, existing) -> {
            final Set<String> resourceNames = tup.v2();
            final Automaton patterns = Automatons.patterns(resourceNames);
            if (existing == null) {
                return new PermissionEntry(appPriv, resourceNames, patterns);
            } else {
                return new PermissionEntry(
                    appPriv,
                    Sets.union(existing.resourceNames, resourceNames),
                    Automatons.unionAndDeterminize(Arrays.asList(existing.resourceAutomaton, patterns))
                );
            }
        }));
        this.permissions = List.copyOf(permissionsByPrivilege.values());
    }

    /**
     * Determines whether this permission grants the specified privilege on the given resource.
     * <p>
     * An {@link ApplicationPermission} consists of a sequence of permission entries, where each entry contains a single
     * {@link ApplicationPrivilege} and one or more resource patterns.
     * </p>
     * <p>
     * This method returns {@code true} if, one or more of those entries meet the following criteria
     * </p>
     * <ul>
     * <li>The entry's application, when interpreted as an {@link Automaton} {@link Automatons#pattern(String) pattern} matches the
     * application given in the argument (interpreted as a raw string)
     * </li>
     * <li>The {@link ApplicationPrivilege#getAutomaton automaton that defines the entry's actions} entirely covers the
     * automaton given in the argument (that is, the argument is a subset of the entry's automaton)
     * </li>
     * <li>The entry's resources, when interpreted as an {@link Automaton} {@link Automatons#patterns(String...)} set of patterns} entirely
     * covers the resource given in the argument (also interpreted as an {@link Automaton} {@link Automatons#pattern(String) pattern}.
     * </li>
     * </ul>
     */
    public boolean grants(ApplicationPrivilege other, String resource) {
        Automaton resourceAutomaton = Automatons.patterns(resource);
        final boolean matched = permissions.stream().anyMatch(e -> e.grants(other, resourceAutomaton));
        logger.trace("Permission [{}] {} grant [{} , {}]", this, matched ? "does" : "does not", other, resource);
        return matched;
    }

    /**
     * For a given application, checks for the privileges for resources and returns an instance of {@link ResourcePrivilegesMap} holding a
     * map of resource to {@link ResourcePrivileges} where the resource is application resource and the map of application privilege to
     * whether it is allowed or not.
     *
     * @param applicationName checks privileges for the provided application name
     * @param checkForResources check permission grants for the set of resources
     * @param checkForPrivilegeNames check permission grants for the set of privilege names
     * @param storedPrivileges stored {@link ApplicationPrivilegeDescriptor} for an application against which the access checks are
     *        performed
     * @param resourcePrivilegesMapBuilder out-parameter for returning the details on which privilege over which resource is granted or not.
     *                                     Can be {@code null} when no such details are needed so the method can return early, after
     *                                     encountering the first privilege that is not granted over some resource.
     * @return {@code true} when all the privileges are granted over all the resources, or {@code false} otherwise
     */
    public boolean checkResourcePrivileges(
        final String applicationName,
        Set<String> checkForResources,
        Set<String> checkForPrivilegeNames,
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        boolean allMatch = true;
        for (String checkResource : checkForResources) {
            for (String checkPrivilegeName : checkForPrivilegeNames) {
                final Set<String> nameSet = Collections.singleton(checkPrivilegeName);
                final Set<ApplicationPrivilege> checkPrivileges = ApplicationPrivilege.get(applicationName, nameSet, storedPrivileges);
                logger.trace("Resolved privileges [{}] for [{},{}]", checkPrivileges, applicationName, nameSet);
                for (ApplicationPrivilege checkPrivilege : checkPrivileges) {
                    assert Automatons.predicate(applicationName).test(checkPrivilege.getApplication())
                        : "Privilege " + checkPrivilege + " should have application " + applicationName;
                    assert checkPrivilege.name().equals(nameSet) : "Privilege " + checkPrivilege + " should have name " + nameSet;
                    if (grants(checkPrivilege, checkResource)) {
                        if (resourcePrivilegesMapBuilder != null) {
                            resourcePrivilegesMapBuilder.addResourcePrivilege(checkResource, checkPrivilegeName, Boolean.TRUE);
                        }
                    } else {
                        if (resourcePrivilegesMapBuilder != null) {
                            resourcePrivilegesMapBuilder.addResourcePrivilege(checkResource, checkPrivilegeName, Boolean.FALSE);
                            allMatch = false;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        return allMatch;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{privileges=" + permissions + "}";
    }

    public Set<String> getApplicationNames() {
        return permissions.stream().map(e -> e.privilege.getApplication()).collect(Collectors.toSet());
    }

    public Set<ApplicationPrivilege> getPrivileges(String application) {
        return permissions.stream()
            .filter(e -> application.equals(e.privilege.getApplication()))
            .map(e -> e.privilege)
            .collect(Collectors.toSet());
    }

    /**
     * Returns a set of resource patterns that are permitted for the provided privilege.
     * The returned set may include patterns that overlap (e.g. "object/*" and "object/1") and may
     * also include patterns that are defined again a more permissive privilege.
     * e.g. If a permission grants
     * <ul>
     *     <li>"my-app", "read", [ "user/*" ]</li>
     *     <li>"my-app", "all", [ "user/kimchy", "config/*" ]</li>
     * </ul>
     * Then <code>getResourcePatterns( myAppRead )</code> would return <code>"user/*", "user/kimchy", "config/*"</code>.
     */
    public Set<String> getResourcePatterns(ApplicationPrivilege privilege) {
        return permissions.stream()
            .filter(e -> e.matchesPrivilege(privilege))
            .map(e -> e.resourceNames)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }

    private static class PermissionEntry {
        private final ApplicationPrivilege privilege;
        private final Predicate<String> application;
        private final Set<String> resourceNames;
        private final Automaton resourceAutomaton;

        private PermissionEntry(ApplicationPrivilege privilege, Set<String> resourceNames, Automaton resourceAutomaton) {
            this.privilege = privilege;
            this.application = Automatons.predicate(privilege.getApplication());
            this.resourceNames = resourceNames;
            this.resourceAutomaton = resourceAutomaton;
        }

        private boolean grants(ApplicationPrivilege other, Automaton resource) {
            return matchesPrivilege(other) && Automatons.subsetOf(resource, this.resourceAutomaton);
        }

        private boolean matchesPrivilege(ApplicationPrivilege other) {
            if (this.privilege.equals(other)) {
                return true;
            }
            if (this.application.test(other.getApplication()) == false) {
                return false;
            }
            if (Operations.isTotal(privilege.getAutomaton())) {
                return true;
            }
            return Operations.isEmpty(privilege.getAutomaton()) == false
                && Operations.isEmpty(other.getAutomaton()) == false
                && Automatons.subsetOf(other.getAutomaton(), privilege.getAutomaton());
        }

        @Override
        public String toString() {
            return privilege.toString() + ":" + resourceNames;
        }
    }
}
