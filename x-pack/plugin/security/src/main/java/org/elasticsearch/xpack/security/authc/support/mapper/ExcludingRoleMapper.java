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

import java.util.Set;

public class ExcludingRoleMapper implements UserRoleMapper {

    private final UserRoleMapper delegate;
    private final Set<String> rolesToExclude;

    public ExcludingRoleMapper(UserRoleMapper delegate, Set<String> rolesToExclude) {
        this.delegate = delegate;
        this.rolesToExclude = rolesToExclude;
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        delegate.resolveRoles(user, ActionListener.wrap(roles -> listener.onResponse(excludeRoles(roles)), listener::onFailure));
    }

    private Set<String> excludeRoles(Set<String> resolvedRoles) {
        if (rolesToExclude.isEmpty()) {
            return resolvedRoles;
        } else {
            return Sets.difference(resolvedRoles, rolesToExclude);
        }
    }

    @Override
    public void refreshRealmOnChange(CachingRealm realm) {
        delegate.refreshRealmOnChange(realm);
    }
}
