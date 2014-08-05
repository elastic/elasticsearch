/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;

/**
 *
 */
public interface RolesStore {

    Permission.Global permission(String role);

    public static interface Writable extends RolesStore {

        void set(String role, Privilege.Index privilege, String... indices);

        void grant(String role, Privilege.Index privilege, String... indices);

        void revoke(String role, Privilege.Index privileges, String... indices);

        void grant(String role, Privilege.Cluster privilege);

        void revoke(String role, Privilege.Cluster privileges);
    }

}
