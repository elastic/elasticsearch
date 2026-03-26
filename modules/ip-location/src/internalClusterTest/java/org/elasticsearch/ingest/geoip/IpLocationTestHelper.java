/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Utility methods for ip-location internal cluster tests that use the
 * {@link IpLocationService} API directly, without ingest pipelines.
 */
public final class IpLocationTestHelper {

    private IpLocationTestHelper() {}

    /**
     * Calls {@link IpLocationService#requestDownloads} on every node in the cluster.
     * Each node has its own {@code downloadRequested} flag, so all must be set for
     * {@code checkDatabases()} to run cluster-wide — mirroring what
     * {@code IngestIpLocationPlugin.clusterChanged()} does per-node.
     */
    public static void requestDownloads(InternalTestCluster cluster, String projectId) {
        for (IpLocationService service : cluster.getInstances(IpLocationService.class)) {
            service.requestDownloads(projectId);
        }
    }

    /**
     * Asserts that the given database is available and that a lookup for the
     * given IP returns the expected value for the specified field.
     */
    public static void assertDatabaseAvailable(
        InternalTestCluster cluster,
        String projectId,
        String databaseFile,
        String ip,
        String expectedField,
        Object expectedValue
    ) throws IOException {
        IpLocationService service = cluster.getAnyMasterNodeInstance(IpLocationService.class);
        IpDataLookup lookup = service.createIpDataLookup(projectId, databaseFile, null);
        assertNotNull("database [" + databaseFile + "] should be available", lookup);
        Map<String, Object> result = lookup.lookup(ip);
        assertNotNull("lookup for [" + ip + "] in [" + databaseFile + "] should return data", result);
        org.hamcrest.MatcherAssert.assertThat(
            "field [" + expectedField + "] in lookup result",
            result.get(expectedField),
            org.hamcrest.Matchers.equalTo(expectedValue)
        );
    }

    /**
     * Asserts that the given database is not currently available on the cluster
     * (i.e. {@code createIpDataLookup} returns {@code null}).
     */
    public static void assertDatabaseUnavailable(InternalTestCluster cluster, String projectId, String databaseFile) {
        IpLocationService service = cluster.getAnyMasterNodeInstance(IpLocationService.class);
        IpDataLookup lookup = service.createIpDataLookup(projectId, databaseFile, null);
        assertNull("database [" + databaseFile + "] should not be available", lookup);
    }
}
