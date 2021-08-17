/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

public class ClusterPrivilegeTests extends ESTestCase {

    public void testMonitorPrivilegeWillGrantActions() {
        assertGranted(ClusterPrivilegeResolver.MONITOR, EnrichStatsAction.INSTANCE);
    }

    public void testAllPrivilegeWillGrantFeatureReset() {
        assertGranted(ClusterPrivilegeResolver.ALL, ResetFeatureStateAction.INSTANCE);
    }

    public void testManagePrivilegeWillDenyFeatureReset() {
        assertDenied(ClusterPrivilegeResolver.MANAGE, ResetFeatureStateAction.INSTANCE);
    }

    public static void assertGranted(ClusterPrivilege clusterPrivilege, ActionType<?> actionType) {
        assertTrue(clusterPrivilege.buildPermission(ClusterPermission.builder()).build()
            .check(actionType.name(), null, null));
    }

    public static void assertDenied(ClusterPrivilege clusterPrivilege, ActionType<?> actionType) {
        assertFalse(clusterPrivilege.buildPermission(ClusterPermission.builder()).build()
            .check(actionType.name(), null, null));
    }

}
