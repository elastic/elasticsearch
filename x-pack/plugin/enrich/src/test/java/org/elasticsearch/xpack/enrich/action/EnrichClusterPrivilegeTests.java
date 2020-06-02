/*
 *
 *  * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 *  * or more contributor license agreements. Licensed under the Elastic License;
 *  * you may not use this file except in compliance with the Elastic License.
 *
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeTests;

public class EnrichClusterPrivilegeTests extends ESTestCase {

    public void testMonitorPrivilegeWillAllowAction() {
        ClusterPrivilegeTests.assertGranted(ClusterPrivilegeResolver.MONITOR, EnrichCoordinatorStatsAction.INSTANCE);
    }
}
