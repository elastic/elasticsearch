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
import java.util.function.BiFunction;

/**
 * This encapsulates the authorization test for resources.
 * There is an additional test for resources that are missing or that are not a datastream or a backing index.
 */
public class IsResourceAuthorizedPredicate {

    private final BiFunction<String, IndexAbstraction, IndicesPermission.AuthorizedComponents> biPredicate;

    public IsResourceAuthorizedPredicate(BiFunction<String, IndexAbstraction, IndicesPermission.AuthorizedComponents> biPredicate) {
        this.biPredicate = biPredicate;
    }

    /**
     * Given another {@link IsResourceAuthorizedPredicate} instance in {@param other},
     * return a new {@link IsResourceAuthorizedPredicate} instance that is equivalent to the conjunction of
     * authorization tests of that other instance and this one.
     */
    // @Override
    public final IsResourceAuthorizedPredicate orAllowIf(IsResourceAuthorizedPredicate other) {
        Objects.requireNonNull(other);
        return new IsResourceAuthorizedPredicate((name, abstraction) -> {
            IndicesPermission.AuthorizedComponents authResult = this.biPredicate.apply(name, abstraction);
            // If we're only authorized for some components, other predicates might authorize us for the rest
            return switch (authResult) {
                case null -> other.biPredicate.apply(name, abstraction);
                case NONE -> other.biPredicate.apply(name, abstraction); // Can't do worse than totally unauthorized, thank u NEXT
                case ALL -> IndicesPermission.AuthorizedComponents.ALL; // Can't do better than ALL, so short circuit
                case DATA, FAILURES -> authResult.union(other.biPredicate.apply(name, abstraction));
            };
        });
    }

    public final IsResourceAuthorizedPredicate alsoRequire(IsResourceAuthorizedPredicate other) {
        Objects.requireNonNull(other);
        return new IsResourceAuthorizedPredicate((name, abstraction) -> {
            IndicesPermission.AuthorizedComponents authResult = this.biPredicate.apply(name, abstraction);
            // If we're only authorized for some components, other predicates might authorize us for the rest
            return switch (authResult) {
                case null -> IndicesPermission.AuthorizedComponents.NONE;
                case NONE -> IndicesPermission.AuthorizedComponents.NONE;
                case ALL -> other.biPredicate.apply(name, abstraction);
                case DATA, FAILURES -> authResult.intersection(other.biPredicate.apply(name, abstraction));
            };
        });
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

    /**
     * Verifies if access is authorized to the resource with the given {@param name}.
     * The {@param indexAbstraction}, which is the resource to be accessed, must be supplied if the resource exists or be {@code null}
     * if it doesn't.
     * Returns {@code true} if access to the given resource is authorized or {@code false} otherwise.
     */
    public IndicesPermission.AuthorizedComponents check(String name, @Nullable IndexAbstraction indexAbstraction) {
        return biPredicate.apply(name, indexAbstraction);
    }
}
