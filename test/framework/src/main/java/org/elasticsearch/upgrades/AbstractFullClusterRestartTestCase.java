/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Map;

public abstract class AbstractFullClusterRestartTestCase extends ESRestTestCase {

    private final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));

    public final boolean isRunningAgainstOldCluster() {
        return runningAgainstOldCluster;
    }

    private final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    public final Version getOldClusterVersion() {
        return oldClusterVersion;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSLMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }


    protected static void assertNoFailures(Map<?, ?> response) {
        int failed = (int) XContentMapValues.extractValue("_shards.failed", response);
        assertEquals(0, failed);
    }


    protected static int extractTotalHits(Map<?, ?> response) {
        return (Integer) XContentMapValues.extractValue("hits.total.value", response);
    }

}
