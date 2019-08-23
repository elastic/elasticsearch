/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
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
     * Checks permission to a cluster action for a given request in the context of given
     * authentication.
     *
     * @param action  cluster action
     * @param request {@link TransportRequest}
     * @param authentication {@link Authentication}
     * @return {@code true} if the access is allowed else returns {@code false}
     */
    public boolean check(final String action, final TransportRequest request, final Authentication authentication) {
        return checks.stream().anyMatch(permission -> permission.check(action, request, authentication));
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
            final Automaton actionAutomaton = createAutomaton(allowedActionPatterns, excludeActionPatterns);
            this.actionAutomatons.add(actionAutomaton);
            return this;
        }

        public Builder add(final ClusterPrivilege clusterPrivilege, final Set<String> allowedActionPatterns,
                           final Predicate<TransportRequest> requestPredicate) {
            final Automaton actionAutomaton = createAutomaton(allowedActionPatterns, Set.of());
            return add(clusterPrivilege, new ActionRequestBasedPermissionCheck(clusterPrivilege, actionAutomaton, requestPredicate));
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

        private static Automaton createAutomaton(Set<String> allowedActionPatterns, Set<String> excludeActionPatterns) {
            allowedActionPatterns = (allowedActionPatterns == null) ? Set.of() : allowedActionPatterns;
            excludeActionPatterns = (excludeActionPatterns == null) ? Set.of() : excludeActionPatterns;

            if (allowedActionPatterns.isEmpty()) {
                return Automatons.EMPTY;
            } else if (excludeActionPatterns.isEmpty()) {
                return Automatons.patterns(allowedActionPatterns);
            } else {
                final Automaton allowedAutomaton = Automatons.patterns(allowedActionPatterns);
                final Automaton excludedAutomaton = Automatons.patterns(excludeActionPatterns);
                return Automatons.minusAndMinimize(allowedAutomaton, excludedAutomaton);
            }
        }
    }

    /**
     * Evaluates whether the cluster actions (optionally for a given request)
     * is permitted by this permission.
     */
    public interface PermissionCheck {
        /**
         * Checks permission to a cluster action for a given request in the context of given
         * authentication.
         *
         * @param action  action name
         * @param request {@link TransportRequest}
         * @param authentication {@link Authentication}
         * @return {@code true} if the specified action for given request is allowed else returns {@code false}
         */
        boolean check(String action, TransportRequest request, Authentication authentication);

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

    /**
     * Base for implementing cluster action based {@link PermissionCheck}.
     * It enforces the checks at cluster action level and then hands it off to the implementations
     * to enforce checks based on {@link TransportRequest} and/or {@link Authentication}.
     */
    public abstract static class ActionBasedPermissionCheck implements PermissionCheck {
        private final Automaton automaton;
        private final Predicate<String> actionPredicate;

        public ActionBasedPermissionCheck(final Automaton automaton) {
            this.automaton = automaton;
            this.actionPredicate = Automatons.predicate(automaton);
        }

        @Override
        public final boolean check(final String action, final TransportRequest request, final Authentication authentication) {
            return actionPredicate.test(action) && extendedCheck(action, request, authentication);
        }

        protected abstract boolean extendedCheck(String action, TransportRequest request, Authentication authentication);

        @Override
        public final boolean implies(final PermissionCheck permissionCheck) {
            if (permissionCheck instanceof ActionBasedPermissionCheck) {
                return Operations.subsetOf(((ActionBasedPermissionCheck) permissionCheck).automaton, this.automaton) &&
                    doImplies((ActionBasedPermissionCheck) permissionCheck);
            }
            return false;
        }

        protected abstract boolean doImplies(ActionBasedPermissionCheck permissionCheck);
    }

    // Automaton based permission check
    private static class AutomatonPermissionCheck extends ActionBasedPermissionCheck {

        AutomatonPermissionCheck(final Automaton automaton) {
            super(automaton);
        }

        @Override
        protected boolean extendedCheck(String action, TransportRequest request, Authentication authentication) {
            return true;
        }

        @Override
        protected boolean doImplies(ActionBasedPermissionCheck permissionCheck) {
            return permissionCheck instanceof AutomatonPermissionCheck;
        }

    }

    // action, request based permission check
    private static class ActionRequestBasedPermissionCheck extends ActionBasedPermissionCheck {
        private final ClusterPrivilege clusterPrivilege;
        private final Predicate<TransportRequest> requestPredicate;

        ActionRequestBasedPermissionCheck(ClusterPrivilege clusterPrivilege, final Automaton automaton,
                                          final Predicate<TransportRequest> requestPredicate) {
            super(automaton);
            this.requestPredicate = requestPredicate;
            this.clusterPrivilege = clusterPrivilege;
        }

        @Override
        protected boolean extendedCheck(String action, TransportRequest request, Authentication authentication) {
            return requestPredicate.test(request);
        }

        @Override
        protected boolean doImplies(final ActionBasedPermissionCheck permissionCheck) {
            if (permissionCheck instanceof ActionRequestBasedPermissionCheck) {
                final ActionRequestBasedPermissionCheck otherCheck =
                    (ActionRequestBasedPermissionCheck) permissionCheck;
                return this.clusterPrivilege.equals(otherCheck.clusterPrivilege);
            }
            return false;
        }
    }
}
