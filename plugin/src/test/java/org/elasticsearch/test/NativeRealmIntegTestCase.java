/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

/**
 * Test case with method to handle the starting and stopping the stores for native users and roles
 */
public abstract class NativeRealmIntegTestCase extends SecurityIntegTestCase {

    @Before
    public void ensureNativeStoresStarted() throws Exception {
        for (NativeUsersStore store : internalCluster().getInstances(NativeUsersStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(NativeUsersStore.State.STARTED));
                }
            });
        }

        for (NativeRolesStore store : internalCluster().getInstances(NativeRolesStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(NativeRolesStore.State.STARTED));
                }
            });
        }
    }

    @After
    public void stopESNativeStores() throws Exception {
        for (NativeUsersStore store : internalCluster().getInstances(NativeUsersStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(NativeUsersStore.State.STOPPED, NativeUsersStore.State.FAILED));
                }
            });
            store.reset();
        }

        for (NativeRolesStore store : internalCluster().getInstances(NativeRolesStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(NativeRolesStore.State.STOPPED, NativeRolesStore.State.FAILED));
                }
            });
            store.reset();
        }

        try {
            // this is a hack to clean up the .security index since only the XPack user can delete it
            internalClient().admin().indices().prepareDelete(SecurityTemplateService.SECURITY_INDEX_NAME).get();
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
