/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.Strings;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;

/**
 * This class helps resolving set of cluster privilege names to a {@link ClusterPrivilege}
 */
public final class ClusterPrivilegeResolver {

    public static final Predicate<String> ACTION_MATCHER = DefaultClusterPrivilege.ALL.clusterPrivilege().predicate();

    private static final ConcurrentHashMap<Set<String>, ClusterPrivilege> CACHE =
            new ConcurrentHashMap<>();

    /**
     * For given set of privilege names returns a {@link ClusterPrivilege}
     *
     * @param name set of predefined names in {@link DefaultClusterPrivilege} or a valid
     * cluster action
     * @return a {@link ClusterPrivilege}
     */
    public static ClusterPrivilege resolve(final Set<String> name) {
        if (name == null || name.isEmpty()) {
            return DefaultClusterPrivilege.NONE.clusterPrivilege();
        }
        return CACHE.computeIfAbsent(name, ClusterPrivilegeResolver::resolvePrivileges);
    }

    private static ClusterPrivilege resolvePrivileges(Set<String> name) {
        Set<String> actions = new HashSet<>();
        Set<String> clusterPrivilegeNames = new HashSet<>();
        Set<ClusterPrivilege> clusterPrivileges = new HashSet<>();
        for (String part : name) {
            part = part.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(part)) {
                clusterPrivilegeNames.add(part);
                actions.add(actionToPattern(part));
            } else {
                DefaultClusterPrivilege privilege = DefaultClusterPrivilege.fromString(part);
                if (privilege != null && name.size() == 1) {
                    return privilege.clusterPrivilege();
                } else if (privilege != null) {
                    clusterPrivileges.add(privilege.clusterPrivilege());
                } else {
                    throw new IllegalArgumentException("unknown cluster privilege [" + name + "]. a privilege must be either "
                            + "one of the predefined fixed cluster privileges ["
                            + Strings.collectionToCommaDelimitedString(DefaultClusterPrivilege.names()) + "] "
                            + "or a pattern over one of the available cluster actions");
                }
            }
        }

        if (actions.isEmpty() == false) {
            clusterPrivileges.add(new ClusterPrivilege(clusterPrivilegeNames, patterns(actions)));
        }
        return MergeableClusterPrivilege.merge(clusterPrivileges);
    }

    private static String actionToPattern(String text) {
        return text + "*";
    }
}
