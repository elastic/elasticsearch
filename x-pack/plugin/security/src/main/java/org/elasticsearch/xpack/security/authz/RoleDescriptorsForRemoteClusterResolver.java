/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Collection;
import java.util.Set;

public class RoleDescriptorsForRemoteClusterResolver {

    private final CompositeRolesStore store;

    public RoleDescriptorsForRemoteClusterResolver(CompositeRolesStore store) {
        this.store = store;
    }

    public void resolve(Authentication authentication, String remoteClusterAlias, ActionListener<RoleDescriptorsIntersection> listener) {
        store.getRoleDescriptorsList(
            authentication.getEffectiveSubject(),
            ActionListener.<Collection<Set<RoleDescriptor>>>wrap(
                rds -> { rds.stream().flatMap(rd -> { return rd.stream(); }).filter(roleDescriptor -> { return true; }); },
                listener::onFailure
            )
        );
    }
}
