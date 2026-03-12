/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of role mapper which wraps a {@link UserRoleMapper}
 * and filters out the resolved roles by removing the configured roles to exclude.
 */
public class ExcludingRoleMapper implements UserRoleMapper {

    private final UserRoleMapper delegate;
    private final Set<String> rolesToExclude;

    public ExcludingRoleMapper(UserRoleMapper delegate, Collection<String> rolesToExclude) {
        this.delegate = Objects.requireNonNull(delegate);
        this.rolesToExclude = Set.copyOf(rolesToExclude);
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        delegate.resolveRoles(user, listener.delegateFailureAndWrap((l, r) -> l.onResponse(excludeRoles(r))));
    }

    private Set<String> excludeRoles(Set<String> resolvedRoles) {
        if (rolesToExclude.isEmpty()) {
            return resolvedRoles;
        } else {
            return Sets.difference(resolvedRoles, rolesToExclude);
        }
    }

    @Override
    public void clearRealmCacheOnChange(CachingRealm realm) {
        delegate.clearRealmCacheOnChange(realm);
    }
}
