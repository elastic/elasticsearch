/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authz.permission.Role;

import java.util.HashMap;
import java.util.Map;

/**
 * A composite roles store that combines built in roles, file-based roles, and index-based roles. Checks the built in roles first, then the
 * file roles, and finally the index roles.
 */
public class CompositeRolesStore extends AbstractComponent {

    private final FileRolesStore fileRolesStore;
    private final NativeRolesStore nativeRolesStore;
    private final ReservedRolesStore reservedRolesStore;

    public CompositeRolesStore(Settings settings, FileRolesStore fileRolesStore, NativeRolesStore nativeRolesStore,
                               ReservedRolesStore reservedRolesStore) {
        super(settings);
        this.fileRolesStore = fileRolesStore;
        this.nativeRolesStore = nativeRolesStore;
        this.reservedRolesStore = reservedRolesStore;
    }
    
    private Role getBuildInRole(String role) {
        // builtins first
        Role builtIn = reservedRolesStore.role(role);
        if (builtIn != null) {
            logger.trace("loaded role [{}] from reserved roles store", role);
            return builtIn;
        }

        // Try the file next, then the index if it isn't there
        Role fileRole = fileRolesStore.role(role);
        if (fileRole != null) {
            logger.trace("loaded role [{}] from file roles store", role);
            return fileRole;
        }
        return null;
    }

    public void roles(String role, ActionListener<Role> roleActionListener) {
        Role storedRole = getBuildInRole(role);
        if (storedRole == null) {
            nativeRolesStore.role(role, roleActionListener);
        } else {
            roleActionListener.onResponse(storedRole);
        }
    }


    public Map<String, Object> usageStats() {
        Map<String, Object> usage = new HashMap<>(2);
        usage.put("file", fileRolesStore.usageStats());
        usage.put("native", nativeRolesStore.usageStats());
        return usage;
    }
}
