/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

import java.util.List;

public class ClusterPrivilegeTests extends ESTestCase {

    public void testMonitorPrivilegeWillGrantActions() {
        assertGranted(ClusterPrivilegeResolver.MONITOR, EnrichStatsAction.INSTANCE);
    }

    public void testMonitorPrivilegeGrantsGetTemplateActions() {
        for (var action : List.of(
            GetComponentTemplateAction.INSTANCE,
            GetComposableIndexTemplateAction.INSTANCE,
            GetIndexTemplatesAction.INSTANCE
        )) {
            assertGranted(ClusterPrivilegeResolver.MONITOR, action);
        }
    }

    public static void assertGranted(ClusterPrivilege clusterPrivilege, ActionType<?> actionType) {
        assertTrue(clusterPrivilege.buildPermission(ClusterPermission.builder()).build().check(actionType.name(), null, null));
    }

}
