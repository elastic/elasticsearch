/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A permission that is based on privileges for cluster wide actions, with the optional ability to inspect the request object
 */
public class ClusterPermission {
    public static final ClusterPermission NONE = new ClusterPermission(Set.of(), List.of());

    private final Set<ClusterPrivilege> clusterPrivileges;
    private final List<PermissionCheck> checks;

    private ClusterPermission(final Set<ClusterPrivilege> clusterPrivileges,
                              final List<PermissionCheck> checks) {
        this.clusterPrivileges = Set.copyOf(clusterPrivileges);
        this.checks = List.copyOf(checks);
    }

    /**
     * Checks permission to a cluster action for a given request.
     *
     * @param action  cluster action
     * @param request {@link TransportRequest}
     * @return {@code true} if the access is allowed else returns {@code false}
     */
    public boolean check(final String action, final TransportRequest request) {
        return checks.stream().anyMatch(permission -> permission.check(action, request));
    }

    /**
     * Checks if the specified {@link ClusterPermission}'s actions are implied by this {@link ClusterPermission}
     *
     * @param otherClusterPermission {@link ClusterPermission}
     * @return {@code true} if the specified cluster permissions actions are implied by this cluster permission else returns {@code false}
     */
    public boolean implies(final ClusterPermission otherClusterPermission) {
        if (otherClusterPermission.checks.isEmpty()) {
            return true;
        } else {
            for (PermissionCheck otherPermissionCheck : otherClusterPermission.checks) {
                boolean isImplied = this.checks.stream().anyMatch(thisPermissionCheck -> thisPermissionCheck.implies(otherPermissionCheck));
                if (isImplied == false) {
                    return false;
                }
            }
            return true;
        }
    }

    public Set<ClusterPrivilege> privileges() {
        return clusterPrivileges;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Set<ClusterPrivilege> clusterPrivileges = new HashSet<>();
        private final List<Automaton> actionAutomatons = new ArrayList<>();
        private final List<PermissionCheck> permissionChecks = new ArrayList<>();

        public Builder add(final ClusterPrivilege clusterPrivilege, final Set<String> allowedActionPatterns,
                           final Set<String> excludeActionPatterns) {
            this.clusterPrivileges.add(clusterPrivilege);
            if (allowedActionPatterns.isEmpty() && excludeActionPatterns.isEmpty()) {
                this.actionAutomatons.add(Automatons.EMPTY);
            } else {
                final Automaton allowedAutomaton = Automatons.patterns(allowedActionPatterns);
                final Automaton excludedAutomaton = Automatons.patterns(excludeActionPatterns);
                this.actionAutomatons.add(Automatons.minusAndMinimize(allowedAutomaton, excludedAutomaton));
            }
            return this;
        }

        public Builder add(final ConfigurableClusterPrivilege configurableClusterPrivilege, final Predicate<String> actionPredicate,
                           final Predicate<TransportRequest> requestPredicate) {
            return add(configurableClusterPrivilege, new ActionRequestPredicatePermissionCheck(configurableClusterPrivilege,
                                                                                               actionPredicate,
                                                                                               requestPredicate));
        }

        public Builder add(final ClusterPrivilege clusterPrivilege, final PermissionCheck permissionCheck) {
            this.clusterPrivileges.add(clusterPrivilege);
            this.permissionChecks.add(permissionCheck);
            return this;
        }

        public ClusterPermission build() {
            if (clusterPrivileges.isEmpty()) {
                return NONE;
            }
            List<PermissionCheck> checks = this.permissionChecks;
            if (false == actionAutomatons.isEmpty()) {
                final Automaton mergedAutomaton = Automatons.unionAndMinimize(this.actionAutomatons);
                checks = new ArrayList<>(this.permissionChecks.size() + 1);
                checks.add(new AutomatonPermissionCheck(mergedAutomaton));
                checks.addAll(this.permissionChecks);
            }
            return new ClusterPermission(this.clusterPrivileges, checks);
        }
    }

    /**
     * Evaluates whether the cluster actions (optionally for a given request)
     * is permitted by this permission.
     */
    public interface PermissionCheck {
        /**
         * Checks permission to a cluster action for a given request.
         *
         * @param action  action name
         * @param request {@link TransportRequest}
         * @return {@code true} if the specified action for given request is allowed else returns {@code false}
         */
        boolean check(String action, TransportRequest request);

        /**
         * Checks whether specified {@link PermissionCheck} is implied by this {@link PermissionCheck}.<br>
         * This is important method to be considered during implementation as it compares {@link PermissionCheck}s.
         * If {@code permissionCheck.implies(otherPermissionCheck)}, that means all the actions allowed by {@code otherPermissionCheck}
         * are also allowed by {@code permissionCheck}, irrespective of the request structure.
         *
         * @param otherPermissionCheck {@link PermissionCheck}
         * @return {@code true} if the specified permission is implied by this {@link PermissionCheck} else
         * returns {@code false}
         */
        boolean implies(PermissionCheck otherPermissionCheck);
    }

    // Automaton based permission check
    private static class AutomatonPermissionCheck implements PermissionCheck {
        private final Automaton automaton;
        private final Predicate<String> actionPredicate;

        AutomatonPermissionCheck(final Automaton automaton) {
            this.automaton = automaton;
            this.actionPredicate = Automatons.predicate(automaton);
        }

        @Override
        public boolean check(final String action, final TransportRequest request) {
            return actionPredicate.test(action);
        }

        @Override
        public boolean implies(final PermissionCheck permissionCheck) {
            if (permissionCheck instanceof AutomatonPermissionCheck) {
                return Operations.subsetOf(((AutomatonPermissionCheck) permissionCheck).automaton, this.automaton);
            }
            return false;
        }
    }

    // action and request based permission check
    private static class ActionRequestPredicatePermissionCheck implements PermissionCheck {
        private final ClusterPrivilege clusterPrivilege;
        final Predicate<String> actionPredicate;
        final Predicate<TransportRequest> requestPredicate;

        ActionRequestPredicatePermissionCheck(final ClusterPrivilege clusterPrivilege, final Predicate<String> actionPredicate,
                                              final Predicate<TransportRequest> requestPredicate) {
            this.clusterPrivilege = clusterPrivilege;
            this.actionPredicate = actionPredicate;
            this.requestPredicate = requestPredicate;
        }

        @Override
        public boolean check(final String action, final TransportRequest request) {
            return actionPredicate.test(action) && requestPredicate.test(request);
        }

        @Override
        public boolean implies(final PermissionCheck permissionCheck) {
            if (permissionCheck instanceof ActionRequestPredicatePermissionCheck) {
                final ActionRequestPredicatePermissionCheck otherCheck = (ActionRequestPredicatePermissionCheck) permissionCheck;
                return this.clusterPrivilege.equals(otherCheck.clusterPrivilege);
            }
            return false;
        }
    }
}
