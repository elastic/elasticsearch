/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.security.test.SecurityTestUtils.getClusterIndexHealth;
import static org.mockito.Mockito.mock;

public class NativeRealmTests extends ESTestCase {

    public void testCacheClearOnIndexHealthChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);
        int expectedInvalidation = 0;
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        RealmConfig config = new RealmConfig("native", Settings.EMPTY, settings, TestEnvironment.newEnvironment(settings),
                new ThreadContext(settings));
        final NativeRealm nativeRealm = new NativeRealm(config, mock(NativeUsersStore.class)) {
            @Override
            void clearCache() {
                numInvalidation.incrementAndGet();
            }
        };

        // existing to no longer present
        ClusterIndexHealth previousHealth = getClusterIndexHealth(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        ClusterIndexHealth currentHealth = null;
        nativeRealm.onSecurityIndexHealthChange(previousHealth, currentHealth);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // doesn't exist to exists
        previousHealth = null;
        currentHealth = getClusterIndexHealth(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        nativeRealm.onSecurityIndexHealthChange(previousHealth, currentHealth);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green or yellow to red
        previousHealth = getClusterIndexHealth(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentHealth = getClusterIndexHealth(ClusterHealthStatus.RED);
        nativeRealm.onSecurityIndexHealthChange(previousHealth, currentHealth);
        assertEquals(expectedInvalidation, numInvalidation.get());

        // red to non red
        previousHealth = getClusterIndexHealth(ClusterHealthStatus.RED);
        currentHealth = getClusterIndexHealth(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        nativeRealm.onSecurityIndexHealthChange(previousHealth, currentHealth);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green to yellow or yellow to green
        previousHealth = getClusterIndexHealth(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentHealth = getClusterIndexHealth(
                previousHealth.getStatus() == ClusterHealthStatus.GREEN ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN);
        nativeRealm.onSecurityIndexHealthChange(previousHealth, currentHealth);
        assertEquals(expectedInvalidation, numInvalidation.get());
    }
}
