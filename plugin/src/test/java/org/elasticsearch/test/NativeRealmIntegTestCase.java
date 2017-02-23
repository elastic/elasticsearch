/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.junit.After;
import org.junit.Before;

/**
 * Test case with method to handle the starting and stopping the stores for native users and roles
 */
public abstract class NativeRealmIntegTestCase extends SecurityIntegTestCase {

    @Before
    public void ensureNativeStoresStarted() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void stopESNativeStores() throws Exception {
        try {
            // this is a hack to clean up the .security index since only the XPack user can delete it
            internalClient().admin().indices().prepareDelete(SecurityLifecycleService.SECURITY_INDEX_NAME).get();
        } catch (IndexNotFoundException e) {
            // ignore it since not all tests create this index...
        }

        if (getCurrentClusterScope() == Scope.SUITE) {
            // Clear the realm cache for all realms since we use a SUITE scoped cluster
            SecurityClient client = securityClient(internalCluster().transportClient());
            client.prepareClearRealmCache().get();
        }
    }
}
