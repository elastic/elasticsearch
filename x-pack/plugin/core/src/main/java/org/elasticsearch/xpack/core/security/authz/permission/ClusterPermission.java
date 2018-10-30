/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
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

    public abstract boolean check(String action, TransportRequest request);

    public abstract List<Tuple<ClusterPrivilege, ConditionalClusterPrivilege>> privileges();

    /**
     * A permission that is based solely on cluster privileges and does not consider request state
     */
    public static class SimpleClusterPermission extends ClusterPermission {

        public static final SimpleClusterPermission NONE = new SimpleClusterPermission(ClusterPrivilege.NONE);

        private final Predicate<String> predicate;

        SimpleClusterPermission(ClusterPrivilege privilege) {
            super(privilege);
            this.predicate = privilege.predicate();
        }

        @Override
        public boolean check(String action, TransportRequest request) {
            return predicate.test(action);
        }

        @Override
        public List<Tuple<ClusterPrivilege, ConditionalClusterPrivilege>> privileges() {
            return Collections.singletonList(new Tuple<>(super.privilege, null));
        }
    }

    /**
     * A permission that makes use of both cluster privileges and request inspection
     */
    public static class ConditionalClusterPermission extends ClusterPermission {
        private final ConditionalClusterPrivilege conditionalPrivilege;

        public ConditionalClusterPermission(ConditionalClusterPrivilege conditionalPrivilege) {
            super(conditionalPrivilege.getPrivilege());
            this.conditionalPrivilege = conditionalPrivilege;
        }

        @Override
        public boolean check(String action, TransportRequest request) {
            return super.privilege.predicate().test(action) && conditionalPrivilege.getRequestPredicate().test(request);
        }

        @Override
        public List<Tuple<ClusterPrivilege, ConditionalClusterPrivilege>> privileges() {
            return Collections.singletonList(new Tuple<>(super.privilege, conditionalPrivilege));
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
            final Set<String> names = children.stream()
                .map(ClusterPermission::privilege)
                .map(ClusterPrivilege::name)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
            return ClusterPrivilege.get(names);
        }

        @Override
        public List<Tuple<ClusterPrivilege, ConditionalClusterPrivilege>> privileges() {
            return children.stream().map(ClusterPermission::privileges).flatMap(List::stream).collect(Collectors.toList());
        }

        @Override
        public boolean check(String action, TransportRequest request) {
            return children.stream().anyMatch(p -> p.check(action, request));
        }
    }
}
