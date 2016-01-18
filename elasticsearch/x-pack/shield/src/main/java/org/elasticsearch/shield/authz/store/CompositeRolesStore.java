/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;

/**
 * A composite roles store that combines file-based and index-based roles
 * lookups. Checks the file first, then the index.
 */
public class CompositeRolesStore implements RolesStore {

    private final FileRolesStore fileRolesStore;
    private final ESNativeRolesStore nativeRolesStore;
    
    @Inject
    public CompositeRolesStore(FileRolesStore fileRolesStore, ESNativeRolesStore nativeRolesStore) {
        this.fileRolesStore = fileRolesStore;
        this.nativeRolesStore = nativeRolesStore;
    }
    
    public Permission.Global.Role role(String role) {
        // Try the file first, then the index if it isn't there
        Permission.Global.Role fileRole = fileRolesStore.role(role);
        if (fileRole != null) {
            return fileRole;
        }

        return nativeRolesStore.role(role);
    }
}
