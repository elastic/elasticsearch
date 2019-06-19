/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.DefaultClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.MergeableClusterPrivilege;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A permission that is based on privileges for cluster wide actions, with the optional ability to inspect the request object
 */
public abstract class ClusterPermission {
    private final ClusterPrivilege privilege;

    ClusterPermission(ClusterPrivilege privilege) {
        this.privilege = privilege;
    }

    public ClusterPrivilege privilege() {
        return privilege;
    }

    public abstract boolean check(String action, TransportRequest request, Authentication authentication);

    public boolean grants(ClusterPrivilege clusterPrivilege) {
        return Operations.subsetOf(clusterPrivilege.getAutomaton(), this.privilege().getAutomaton());
    }

    public abstract List<ClusterPrivilege> privileges();

    /**
     * A permission that is based solely on cluster privileges and does not consider request state
     */
    public static class SimpleClusterPermission extends ClusterPermission {

        public static final SimpleClusterPermission NONE = new SimpleClusterPermission(DefaultClusterPrivilege.NONE.clusterPrivilege());

        SimpleClusterPermission(ClusterPrivilege privilege) {
            super(privilege);
        }

        @Override
        public boolean check(String action, TransportRequest request, Authentication authentication) {
            boolean allowed = this.privilege().predicate().test(action);
            if (this.privilege() instanceof ConditionalClusterPrivilege) {
                allowed = allowed && ((ConditionalClusterPrivilege) this.privilege()).getRequestPredicate().test(request, authentication);
            }
            return allowed;
        }

        @Override
        public List<ClusterPrivilege> privileges() {
            return Collections.singletonList(super.privilege);
        }
    }

    /**
     * A permission that makes use of both cluster privileges and request inspection
     */
    public static class ConditionalClusterPermission extends ClusterPermission {

        public ConditionalClusterPermission(ConditionalClusterPrivilege conditionalPrivilege) {
            super(conditionalPrivilege);
        }

        @Override
        public boolean check(String action, TransportRequest request, Authentication authentication) {
            return privilege().predicate().test(action)
                    && ((ConditionalClusterPrivilege) privilege()).getRequestPredicate().test(request, authentication);
        }

        @Override
        public List<ClusterPrivilege> privileges() {
            return Collections.singletonList(privilege());
        }
    }

    /**
     * A permission that composes a number of other cluster permissions
     */
    public static class CompositeClusterPermission extends ClusterPermission {
        private final Collection<ClusterPermission> children;

        public CompositeClusterPermission(Collection<ClusterPermission> children) {
            super(buildPrivilege(children));
            this.children = children;
        }

        private static ClusterPrivilege buildPrivilege(Collection<ClusterPermission> children) {
            return MergeableClusterPrivilege.merge(privileges(children));
        }

        private static List<ClusterPrivilege> privileges(Collection<ClusterPermission> children) {
            return children.stream().map(ClusterPermission::privileges).flatMap(List::stream).collect(Collectors.toList());
        }

        @Override
        public List<ClusterPrivilege> privileges() {
            return privileges(children);
        }

        @Override
        public boolean check(String action, TransportRequest request, Authentication authentication) {
            return children.stream().anyMatch(p -> p.check(action, request, authentication));
        }

    }
}
