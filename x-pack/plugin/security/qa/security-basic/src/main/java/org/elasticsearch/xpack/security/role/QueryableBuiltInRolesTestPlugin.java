/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.util.List;

public class QueryableBuiltInRolesTestPlugin extends Plugin {

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ReservedRolesStore.INCLUDED_RESERVED_ROLES_SETTING);
    }
}
