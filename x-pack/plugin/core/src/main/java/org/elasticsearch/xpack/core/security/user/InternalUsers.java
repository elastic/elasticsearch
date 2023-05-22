/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InternalUsers {

    /**
     * "Async Search" internal user - used to manage async search tasks and write results to the internal results system index
     */
    public static final InternalUser ASYNC_SEARCH_USER = new InternalUser(
        UsernamesField.ASYNC_SEARCH_NAME,
        new RoleDescriptor(
            UsernamesField.ASYNC_SEARCH_ROLE,
            new String[] { "cancel_task" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(XPackPlugin.ASYNC_RESULTS_INDEX + "*")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build(), },
            null,
            null,
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            null
        )
    );

    /**
     * internal user that manages the security profile index. Has no cluster permission.
     */
    public static final InternalUser SECURITY_PROFILE_USER = new InternalUser(
        UsernamesField.SECURITY_PROFILE_NAME,
        new RoleDescriptor(
            UsernamesField.SECURITY_PROFILE_ROLE,
            null,
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".security-profile", "/\\.security-profile-[0-9].*/")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build() },
            null,
            null,
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            Map.of()
        )
    );

    /**
    * "Storage" internal user - used when the indexing/storage subsystem needs to perform actions on specific indices
    * (that may not be permitted by the authenticated user)
    */
    public static final InternalUser STORAGE_USER = new InternalUser(
        UsernamesField.STORAGE_USER_NAME,
        new RoleDescriptor(
            UsernamesField.STORAGE_ROLE_NAME,
            new String[] {},
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("*")
                    .privileges(RefreshAction.NAME + "*")
                    .allowRestrictedIndices(true)
                    .build() },
            new String[] {},
            MetadataUtils.DEFAULT_RESERVED_METADATA
        )
    );

    /**
     * XPack internal user that manages xpack. Has all cluster/indices permissions for x-pack to operate excluding security permissions.
     */
    public static final InternalUser XPACK_USER = new InternalUser(
        UsernamesField.XPACK_NAME,
        new RoleDescriptor(
            UsernamesField.XPACK_ROLE,
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("/@&~(\\.security.*)&~(\\.async-search.*)/")
                    .privileges("all")
                    .allowRestrictedIndices(true)
                    .build() },
            new String[] { "*" },
            MetadataUtils.DEFAULT_RESERVED_METADATA
        )
    );

    /**
     * DLM internal user that manages DLM. Has all indices permissions to perform DLM runtime tasks.
     */
    public static final InternalUser DLM_USER = new InternalUser(
        UsernamesField.DLM_NAME,
        new RoleDescriptor(
            UsernamesField.DLM_ROLE,
            new String[] {},
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("/@&~(\\.security.*)&~(\\.async-search.*)/")
                    // TODO set correct privileges here
                    .privileges("none")
                    // TODO sanity check
                    .allowRestrictedIndices(true)
                    .build() },
            null,
            null,
            new String[] {},
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            Map.of()
        )
    );

    /**
     * internal user that manages xpack security. Has all cluster/indices permissions.
     */
    public static final InternalUser XPACK_SECURITY_USER = new InternalUser(
        UsernamesField.XPACK_SECURITY_NAME,
        new RoleDescriptor(
            UsernamesField.XPACK_SECURITY_ROLE,
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(true).build() },
            null,
            null,
            new String[] { "*" },
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            Map.of()
        )
    );

    private static final Map<String, InternalUser> INTERNAL_USERS = new HashMap<>();

    static {
        defineUser(SystemUser.INSTANCE);
        defineUser(XPACK_USER);
        defineUser(XPACK_SECURITY_USER);
        defineUser(SECURITY_PROFILE_USER);
        defineUser(ASYNC_SEARCH_USER);
        defineUser(CrossClusterAccessUser.INSTANCE);
        defineUser(STORAGE_USER);
        defineUser(DLM_USER);
    }

    private static void defineUser(InternalUser user) {
        INTERNAL_USERS.put(user.principal(), user);
    }

    public static Collection<InternalUser> get() {
        return Collections.unmodifiableCollection(INTERNAL_USERS.values());
    }

    public static InternalUser getUser(String username) {
        final var instance = INTERNAL_USERS.get(username);
        if (instance == null) {
            throw new IllegalStateException("user [" + username + "] is not internal");
        }
        return instance;
    }
}
