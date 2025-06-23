/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.simulatedlatencyrepo;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class LatencySimulatingRespositoryPluginTests extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(LatencySimulatingRepositoryPlugin.class);
        return plugins;
    }

    public void testRepositoryCanBeConfigured() {
        String dataNode = internalCluster().startDataOnlyNode();
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        createRepository(repositoryName, "latency-simulating", Settings.builder().put("location", randomRepoPath()).put("latency", 150));

        Repository repo = getRepositoryOnNode(repositoryName, dataNode);
        assertThat(repo, instanceOf(LatencySimulatingBlobStoreRepository.class));

        disableRepoConsistencyCheck("This test checks an empty repository");
    }
}
