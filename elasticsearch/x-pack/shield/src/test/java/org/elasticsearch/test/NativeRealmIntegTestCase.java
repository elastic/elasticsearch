/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.shield.ShieldTemplateService;
import org.elasticsearch.shield.authc.esnative.ESNativeUsersStore;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.shield.client.SecurityClient;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

/**
 * Test case with method to handle the starting and stopping the stores for native users and roles
 */
public abstract class NativeRealmIntegTestCase extends ShieldIntegTestCase {

    @Before
    public void ensureNativeStoresStarted() throws Exception {
        for (ESNativeUsersStore store : internalCluster().getInstances(ESNativeUsersStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(ESNativeUsersStore.State.STARTED));
                }
            });
        }

        for (ESNativeRolesStore store : internalCluster().getInstances(ESNativeRolesStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(ESNativeRolesStore.State.STARTED));
                }
            });
        }
    }

    @After
    public void stopESNativeStores() throws Exception {
        for (ESNativeUsersStore store : internalCluster().getInstances(ESNativeUsersStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(ESNativeUsersStore.State.STOPPED, ESNativeUsersStore.State.FAILED));
                }
            });
            store.reset();
        }

        for (ESNativeRolesStore store : internalCluster().getInstances(ESNativeRolesStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(ESNativeRolesStore.State.STOPPED, ESNativeRolesStore.State.FAILED));
                }
            });
            store.reset();
        }

        try {
            // this is a hack to clean up the .security index since only the XPack user can delete it
            internalClient().admin().indices().prepareDelete(ShieldTemplateService.SECURITY_INDEX_NAME).get();
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
