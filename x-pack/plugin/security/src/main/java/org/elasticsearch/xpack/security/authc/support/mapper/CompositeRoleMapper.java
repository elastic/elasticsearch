/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link UserRoleMapper} that composes one or more <i>delegate</i> role-mappers.
 * During {@link #resolveRoles(UserData, ActionListener) role resolution}, each of the delegates is
 * queried, and the individual results are merged into a single {@link Set} which includes all the roles from each mapper.
 */
public class CompositeRoleMapper implements UserRoleMapper {

    private final List<UserRoleMapper> delegates;

    public CompositeRoleMapper(UserRoleMapper... delegates) {
        this.delegates = new ArrayList<>(Arrays.asList(delegates));
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        GroupedActionListener<Set<String>> groupListener = new GroupedActionListener<>(
            delegates.size(),
            ActionListener.wrap(
                composite -> listener.onResponse(composite.stream().flatMap(Set::stream).collect(Collectors.toSet())),
                listener::onFailure
            )
        );
        this.delegates.forEach(mapper -> mapper.resolveRoles(user, groupListener));
    }

    @Override
    public void clearRealmCacheOnChange(CachingRealm realm) {
        this.delegates.forEach(mapper -> mapper.clearRealmCacheOnChange(realm));
    }
}
