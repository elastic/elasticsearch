/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Merges list of {@link ClusterPrivilege} into a single cluster privilege.
 * If the list contains {@link ConditionalClusterPrivilege} then the resultant is also a {@link ConditionalClusterPrivilege}
 * else it will be plain {@link ClusterPrivilege}
 */
public class MergeableClusterPrivilege extends ConditionalClusterPrivilege {

    private BiPredicate<TransportRequest, Authentication> conditionalPredicate;

    private MergeableClusterPrivilege(Set<String> name, Automaton automaton,
                                      BiPredicate<TransportRequest, Authentication> conditionalPredicate) {
        super(name, automaton);
        this.conditionalPredicate = conditionalPredicate;
    }

    /**
     * The request-level privilege (as a {@link BiPredicate}) that is required by this conditional privilege. Conditions can also be
     * evaluated based on the {@link Authentication} details.
     */
    @Override
    public BiPredicate<TransportRequest, Authentication> getRequestPredicate() {
        return conditionalPredicate;
    }

    public static ClusterPrivilege merge(Collection<ClusterPrivilege> clusterPrivileges) {
        Set<String> names = new HashSet<>();
        Set<Automaton> automatons = new HashSet<>();
        BiPredicate<TransportRequest, Authentication> conditionalPredicate = null;
        for (ClusterPrivilege clusterPrivilege : clusterPrivileges) {
            names.addAll(clusterPrivilege.name());
            automatons.add(clusterPrivilege.getAutomaton());
            if (clusterPrivilege instanceof ConditionalClusterPrivilege) {
                if (conditionalPredicate == null) {
                    conditionalPredicate = ((ConditionalClusterPrivilege) clusterPrivilege).getRequestPredicate();
                } else {
                    conditionalPredicate = conditionalPredicate.or(((ConditionalClusterPrivilege) clusterPrivilege).getRequestPredicate());
                }
            }
        }
        return (conditionalPredicate != null)
                ? new MergeableClusterPrivilege(names, Automatons.unionAndMinimize(automatons), conditionalPredicate)
                : new ClusterPrivilege(names, Automatons.unionAndMinimize(automatons));
    }
}
