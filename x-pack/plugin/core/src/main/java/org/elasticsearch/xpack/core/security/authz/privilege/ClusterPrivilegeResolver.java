/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;

/**
 * This class helps resolving set of cluster privilege names to a {@link Tuple} of {@link ClusterPrivilege} or
 * {@link ConditionalClusterPrivilege}
 */
public final class ClusterPrivilegeResolver {

    public static final Predicate<String> ACTION_MATCHER = DefaultClusterPrivilege.ALL.predicate();

    private static final ConcurrentHashMap<Set<String>, Tuple<ClusterPrivilege, Set<ConditionalClusterPrivilege>>> CACHE =
            new ConcurrentHashMap<>();

    /**
     * For given set of privilege names returns a tuple of {@link ClusterPrivilege} and set of predefined fixed conditional cluster
     * privileges {@link ConditionalClusterPrivilege}
     *
     * @param name set of predefined names in {@link DefaultClusterPrivilege} or {@link DefaultConditionalClusterPrivilege} or a valid
     * cluster action
     * @return a {@link Tuple} of {@link ClusterPrivilege} and set of predefined fixed conditional cluster privileges
     * {@link ConditionalClusterPrivilege}
     */
    public static Tuple<ClusterPrivilege, Set<ConditionalClusterPrivilege>> resolve(final Set<String> name) {
        if (name == null || name.isEmpty()) {
            return new Tuple<ClusterPrivilege, Set<ConditionalClusterPrivilege>>(DefaultClusterPrivilege.NONE.clusterPrivilege(),
                    Collections.emptySet());
        }
        return CACHE.computeIfAbsent(name, ClusterPrivilegeResolver::resolveI);
    }

    private static Tuple<ClusterPrivilege, Set<ConditionalClusterPrivilege>> resolveI(Set<String> name) {
        final int size = name.size();
        if (size == 0) {
            throw new IllegalArgumentException("empty set should not be used");
        }

        Set<String> actions = new HashSet<>();
        Set<Automaton> automata = new HashSet<>();
        Set<ConditionalClusterPrivilege> conditionalClusterPrivileges = new HashSet<>();
        for (String part : name) {
            part = part.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(part)) {
                actions.add(actionToPattern(part));
            } else {
                DefaultClusterPrivilege privilege = DefaultClusterPrivilege.fromString(part);
                if (privilege != null && size == 1) {
                    return new Tuple<ClusterPrivilege, Set<ConditionalClusterPrivilege>>(privilege.clusterPrivilege(),
                            Collections.emptySet());
                } else if (privilege != null) {
                    automata.add(privilege.automaton());
                } else {
                    DefaultConditionalClusterPrivilege dccp = DefaultConditionalClusterPrivilege.fromString(part);
                    if (dccp != null) {
                        conditionalClusterPrivileges.add(dccp.conditionalClusterPrivilege);
                    } else {
                        throw new IllegalArgumentException("unknown cluster privilege [" + name + "]. a privilege must be either " +
                                "one of the predefined fixed cluster privileges [" +
                                Strings.collectionToCommaDelimitedString(DefaultClusterPrivilege.names()) + "], " +
                                "predefined fixed conditional cluster privileges [" +
                                Strings.collectionToCommaDelimitedString(DefaultConditionalClusterPrivilege.names()) + "] " +
                                "or a pattern over one of the available cluster actions");
                    }
                }
            }
        }

        if (actions.isEmpty() == false) {
            automata.add(patterns(actions));
        }
        final ClusterPrivilege clusterPrivilege = new ClusterPrivilege(name, Automatons.unionAndMinimize(automata));
        return new Tuple<ClusterPrivilege, Set<ConditionalClusterPrivilege>>(clusterPrivilege, conditionalClusterPrivileges);
    }

    static String actionToPattern(String text) {
        return text + "*";
    }
}
