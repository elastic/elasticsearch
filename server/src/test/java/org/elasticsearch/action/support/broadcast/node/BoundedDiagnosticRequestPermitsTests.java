/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.action.support.broadcast.node.BoundedDiagnosticRequestPermits.MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE;

public class BoundedDiagnosticRequestPermitsTests extends ESTestCase {

    public void testGrantsPermitsUpToMaxPermits() {
        int maxPermits = randomIntBetween(1, 5);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits = new BoundedDiagnosticRequestPermits(
            Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), maxPermits).build(),
            clusterSettings
        );

        for (int i = 0; i < maxPermits; i++) {
            assertTrue(boundedDiagnosticRequestPermits.tryAcquire());
        }
        assertFalse(boundedDiagnosticRequestPermits.tryAcquire());
        boundedDiagnosticRequestPermits.release();
        assertTrue(boundedDiagnosticRequestPermits.tryAcquire());
    }

    public void testBroadcastRequestPermitCanBeDynamicallyUpdated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits = new BoundedDiagnosticRequestPermits(
            Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 1).build(),
            clusterSettings
        );

        assertTrue(boundedDiagnosticRequestPermits.tryAcquire());
        assertFalse(boundedDiagnosticRequestPermits.tryAcquire());

        clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 2).build());

        assertTrue(boundedDiagnosticRequestPermits.tryAcquire());

        clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 1).build());

        assertFalse(boundedDiagnosticRequestPermits.tryAcquire());
        boundedDiagnosticRequestPermits.release();
        boundedDiagnosticRequestPermits.release();

        assertTrue(boundedDiagnosticRequestPermits.tryAcquire());
        assertFalse(boundedDiagnosticRequestPermits.tryAcquire());
    }

    public void testMaxConcurrentBroadcastRequestsPerNodeIsValidated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings invalidSetting = Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 0).build();
        expectThrows(IllegalArgumentException.class, () -> new BoundedDiagnosticRequestPermits(invalidSetting, clusterSettings));
        new BoundedDiagnosticRequestPermits(
            Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 1).build(),
            clusterSettings
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 0).build()
            )
        );
    }
}
