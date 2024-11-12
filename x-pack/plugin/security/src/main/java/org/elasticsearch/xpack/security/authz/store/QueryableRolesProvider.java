/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Map;

public interface QueryableRolesProvider {

    QueryableRoles roles();

    void addListener(QueryableRolesChangedListener listener);

    record QueryableRoles(Map<String, String> roleVersions, Map<String, RoleDescriptor> roleDescriptors) {}

    interface QueryableRolesChangedListener {
        void onRolesChanged(QueryableRoles roles);
    }
}
