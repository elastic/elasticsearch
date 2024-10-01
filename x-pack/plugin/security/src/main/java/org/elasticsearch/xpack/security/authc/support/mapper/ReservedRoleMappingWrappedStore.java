/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.util.List;
import java.util.Set;

public class ReservedRoleMappingWrappedStore extends AbstractRoleMapperClearRealmCache implements AbstractRoleMappingStore {

    private final ReservedRoleMappings reservedRoleMappings;
    private final NativeRoleMappingStore delegate;

    public ReservedRoleMappingWrappedStore(ReservedRoleMappings reservedRoleMappings, NativeRoleMappingStore delegate) {
        this.reservedRoleMappings = reservedRoleMappings;
        this.delegate = delegate;
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        getRoleMappings(
            null,
            ActionListener.wrap(mappings -> listener.onResponse(delegate.resolveRoles(user, mappings)), listener::onFailure)
        );
    }

    @Override
    public void putRoleMapping(PutRoleMappingRequest request, ActionListener<Boolean> listener) {
        // TODO what if isReserved throws due to cluster-state block/other cluster-state issue?
        if (reservedRoleMappings.isReserved(request.getName())) {
            listener.onFailure(
                new IllegalArgumentException(
                    "Role mapping [" + request.getName() + "] is reserved and cannot be modified via Native Role Mapping APIs"
                )
            );
            return;
        }
        delegate.putRoleMapping(request, listener);
    }

    @Override
    public void deleteRoleMapping(DeleteRoleMappingRequest request, ActionListener<Boolean> listener) {
        // TODO what if isReserved throws due to cluster-state block/other cluster-state issue?
        if (reservedRoleMappings.isReserved(request.getName())) {
            listener.onFailure(
                new IllegalArgumentException(
                    "Role mapping [" + request.getName() + "] is reserved and cannot be modified via Native Role Mapping APIs"
                )
            );
            return;
        }
        delegate.deleteRoleMapping(request, listener);
    }

    @Override
    public void getRoleMappings(Set<String> names, ActionListener<List<ExpressionRoleMapping>> listener) {
        delegate.getRoleMappings(names, listener.delegateFailureAndWrap((l, roleMappings) -> {
            final List<ExpressionRoleMapping> merged = reservedRoleMappings.mergeWithReserved(roleMappings);
            final boolean includeAll = names == null || names.isEmpty();
            if (includeAll) {
                l.onResponse(merged);
            } else {
                l.onResponse(merged.stream().filter(m -> names.contains(m.getName())).toList());
            }
        }));
    }

    @Override
    public void clearRealmCacheOnChange(CachingRealm realm) {
        reservedRoleMappings.clearRealmCacheOnChange(realm);
        delegate.clearRealmCacheOnChange(realm);
    }
}
