/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RemoteClusterAwareRolesStore {

    private final CompositeRolesStore rolesStore;

    public RemoteClusterAwareRolesStore(CompositeRolesStore rolesStore) {
        this.rolesStore = rolesStore;
    }

    public void getRoleDescriptorsIntersection(
        final String remoteClusterAlias,
        final Subject subject,
        final ActionListener<RoleDescriptorsIntersection> listener
    ) {
        rolesStore.getRoleDescriptorsList(subject, ActionListener.wrap(roleDescriptorList -> {
            final Collection<Set<RoleDescriptor>> roleDescriptorGroups = new ArrayList<>();
            for (var roleDescriptorGroup : roleDescriptorList) {
                final Set<RoleDescriptor> roleDescriptors = new HashSet<>();
                for (var roleDescriptor : roleDescriptorGroup) {
                    final List<RoleDescriptor.IndicesPrivileges> indexPrivileges = new ArrayList<>();
                    for (var remoteIndexPrivilege : roleDescriptor.getRemoteIndicesPrivileges()) {
                        if (StringMatcher.of(remoteIndexPrivilege.remoteClusters()).test(remoteClusterAlias)) {
                            indexPrivileges.add(remoteIndexPrivilege.indicesPrivileges());
                        }
                    }
                    if (false == indexPrivileges.isEmpty()) {
                        roleDescriptors.add(
                            new RoleDescriptor(
                                roleDescriptor.getName(),
                                null,
                                indexPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[0]),
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        );
                    }
                }
                if (false == roleDescriptors.isEmpty()) {
                    roleDescriptorGroups.add(roleDescriptors);
                }
            }
            listener.onResponse(new RoleDescriptorsIntersection(roleDescriptorGroups));
        }, listener::onFailure));
    }
}
