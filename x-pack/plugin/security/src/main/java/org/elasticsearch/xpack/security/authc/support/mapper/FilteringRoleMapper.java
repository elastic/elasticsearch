/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class FilteringRoleMapper implements UserRoleMapper {

    private static final Logger logger = LogManager.getLogger(FilteringRoleMapper.class);
    private final FileRolesStore fileRolesStore;
    private final UserRoleMapper delegate;

    public FilteringRoleMapper(FileRolesStore fileRolesStore, UserRoleMapper delegate) {
        this.fileRolesStore = fileRolesStore;
        this.delegate = delegate;
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        delegate.resolveRoles(user, ActionListener.wrap((roles -> {
            if (fileRolesStore == null) {
                logger.info("Skipping filtering role mapper");
                listener.onResponse(roles);
                return;
            }

            final var filtered = roles.stream().filter(roleName -> {
                if (ReservedRolesStore.isReserved(roleName)) {
                    logger.info("Filtered out reserved role [{}]", roleName);
                    return false;
                }
                return true;
            }).collect(Collectors.toSet());
            fileRolesStore.accept(filtered, new ActionListener<>() {
                @Override
                public void onResponse(RoleRetrievalResult roleRetrievalResult) {
                    final Set<String> toRemove = new HashSet<>();
                    for (var roleDescriptor : roleRetrievalResult.getDescriptors()) {
                        if (roleDescriptor.getMetadata().containsKey("_internal")) {
                            logger.info("Filtered out internal role [{}]", roleDescriptor.getName());
                            toRemove.add(roleDescriptor.getName());
                        }
                    }
                    // We want to keep not-found roles
                    listener.onResponse(Sets.difference(filtered, toRemove));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to look-up roles from file", e);
                    listener.onResponse(filtered);
                }
            });
        }), listener::onFailure));
    }

    @Override
    public void refreshRealmOnChange(CachingRealm realm) {
        delegate.refreshRealmOnChange(realm);
    }
}
