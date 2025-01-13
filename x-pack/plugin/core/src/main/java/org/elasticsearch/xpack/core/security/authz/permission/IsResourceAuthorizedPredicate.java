/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * This encapsulates the authorization test for resources.
 * There is an additional test for resources that are missing or that are not a datastream or a backing index.
 */
public abstract class IsResourceAuthorizedPredicate {
    /**
     * A checker that allows access to no resources.
     */
    static class NoResourcesAuthorizedChecker extends IsResourceAuthorizedPredicate {
        @Override
        public IndicesPermission.AuthorizedComponents check(String name, IndexAbstraction indexAbstraction, boolean authByDataStream) {
            return IndicesPermission.AuthorizedComponents.NONE;
        }
    }

    /**
     * A checker which combines the results of two checkers into a single result, authorizing access if either of the two given checkers
     * grant access, and denying only if neither grants access.
     */
    static class OrChecker extends IsResourceAuthorizedPredicate {
        private final IsResourceAuthorizedPredicate a;
        private final IsResourceAuthorizedPredicate b;

        OrChecker(IsResourceAuthorizedPredicate a, IsResourceAuthorizedPredicate b) {
            this.a = Objects.requireNonNull(a);
            this.b = Objects.requireNonNull(b);
        }

        @Override
        public IndicesPermission.AuthorizedComponents check(String name, IndexAbstraction abstraction, boolean authByDataStream) {
            IndicesPermission.AuthorizedComponents authResult = a.check(name, abstraction);
            // If we're only authorized for some components, other predicates might authorize us for the rest
            return switch (authResult) {
                case null -> b.check(name, abstraction, authByDataStream);
                case NONE -> b.check(name, abstraction, authByDataStream); // Can't do worse than totally unauthorized, thank u NEXT
                case ALL -> IndicesPermission.AuthorizedComponents.ALL; // Can't do better than ALL, so short circuit
                case DATA, FAILURES -> authResult.union(b.check(name, abstraction, authByDataStream));
            };
        }
    }

    /**
     * A checker which combines the results of two checkers into a single result, authorizing access only if both of the given checkers
     * authorize access, and denying access otherwise.
     */
    static class AndChecker extends IsResourceAuthorizedPredicate {
        private final IsResourceAuthorizedPredicate a;
        private final IsResourceAuthorizedPredicate b;

        AndChecker(IsResourceAuthorizedPredicate a, IsResourceAuthorizedPredicate b) {
            this.a = Objects.requireNonNull(a);
            this.b = Objects.requireNonNull(b);
        }

        @Override
        public IndicesPermission.AuthorizedComponents check(String name, IndexAbstraction abstraction, boolean authByDataStream) {
            IndicesPermission.AuthorizedComponents authResult = a.check(name, abstraction, authByDataStream);
            // We can short circuit out if anything is unauthorized here
            return switch (authResult) {
                case null -> IndicesPermission.AuthorizedComponents.NONE;
                case NONE -> IndicesPermission.AuthorizedComponents.NONE;
                case ALL -> b.check(name, abstraction, authByDataStream);
                case DATA, FAILURES -> authResult.intersection(b.check(name, abstraction, authByDataStream));
            };
        }
    }

    /**
     * Given another {@link IsResourceAuthorizedPredicate} instance in {@param other},
     * return a new {@link IsResourceAuthorizedPredicate} instance that is equivalent to the conjunction of
     * authorization tests of that other instance and this one.
     */
    public final IsResourceAuthorizedPredicate orAllowIf(IsResourceAuthorizedPredicate other) {
        return new OrChecker(this, other);
    }

    public final IsResourceAuthorizedPredicate alsoRequire(IsResourceAuthorizedPredicate other) {
        return new AndChecker(this, other);
    }

    /**
     * Check which components of the given {@param indexAbstraction} resource is authorized.
     * The resource must exist. Otherwise, use the {@link #check(String, IndexAbstraction)} method.
     *
     * @return An object representing which components of this index abstraction the user is authorized to access
     */
    public final IndicesPermission.AuthorizedComponents check(IndexAbstraction indexAbstraction) {
        return check(indexAbstraction.getName(), indexAbstraction);
    }

    public final boolean test(IndexAbstraction indexAbstraction) {
        IndicesPermission.AuthorizedComponents authResult = check(indexAbstraction);
        return authResult != null && authResult.isDataAuthorized();
    }

    public IndicesPermission.AuthorizedComponents check(String name, @Nullable IndexAbstraction indexAbstraction) {
        return check(name, indexAbstraction, true);
    }

    public abstract IndicesPermission.AuthorizedComponents check(
        String name,
        @Nullable IndexAbstraction indexAbstraction,
        boolean allowAuthorizationViaDataStream
    );
}
