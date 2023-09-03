/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class UnregisteredSettingsIntegTests extends SecurityIntegTestCase {

    public void testIncludeReservedRolesSettingNotRegistered() {
        internalCluster().setBootstrapMasterNodeIndex(0);

        final Settings.Builder builder = Settings.builder()
            .put(randomBoolean() ? masterNode() : dataOnlyNode())
            .putList("xpack.security.reserved_roles.include", "superuser");

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> internalCluster().startNode(builder));
        assertThat(e.getMessage(), containsString("unknown setting"));
    }
}
