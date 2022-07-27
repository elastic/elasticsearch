/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;

public class ApiKeyUserRoleDescriptorResolver {
    private final CompositeRolesStore rolesStore;
    private final NamedXContentRegistry xContentRegistry;

    public ApiKeyUserRoleDescriptorResolver(CompositeRolesStore rolesStore, NamedXContentRegistry xContentRegistry) {
        this.rolesStore = rolesStore;
        this.xContentRegistry = xContentRegistry;
    }

    public void resolveUserRoleDescriptors(final Authentication authentication, final ActionListener<Set<RoleDescriptor>> listener) {
        final ActionListener<Set<RoleDescriptor>> roleDescriptorsListener = ActionListener.wrap(roleDescriptors -> {
            for (RoleDescriptor rd : roleDescriptors) {
                try {
                    DLSRoleQueryValidator.validateQueryField(rd.getIndicesPrivileges(), xContentRegistry);
                } catch (ElasticsearchException | IllegalArgumentException e) {
                    listener.onFailure(e);
                    return;
                }
            }
            listener.onResponse(roleDescriptors);
        }, listener::onFailure);

        final Subject effectiveSubject = authentication.getEffectiveSubject();

        // Retain current behaviour that User of an API key authentication has no roles
        if (effectiveSubject.getType() == Subject.Type.API_KEY) {
            roleDescriptorsListener.onResponse(Set.of());
            return;
        }

        rolesStore.getRoleDescriptorsList(effectiveSubject, ActionListener.wrap(roleDescriptorsList -> {
            assert roleDescriptorsList.size() == 1;
            roleDescriptorsListener.onResponse(roleDescriptorsList.iterator().next());
        }, roleDescriptorsListener::onFailure));
    }
}
