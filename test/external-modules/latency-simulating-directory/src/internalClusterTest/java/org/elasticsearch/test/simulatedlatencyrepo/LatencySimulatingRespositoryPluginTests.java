/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.simulatedlatencyrepo;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LatencySimulatingRespositoryPluginTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LatencySimulatingRepositoryPlugin.class);
    }

    public void testRepositoryCanBeConfigured() {
        // put repository with type 'latency-simulating`
        // index, snapshot and restore
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        final Settings repoSettings = Settings.builder().put("location", location).put("latency", 150).build();

        logger.info("-->  creating repository");
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repositoryName)
            .setType(LatencySimulatingRepositoryPlugin.TYPE)
            .setSettings(repoSettings)
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        RepositoriesService rs = node().injector().getInstance(RepositoriesService.class);
        assertThat(rs.repository(repositoryName), instanceOf(LatencySimulatingBlobStoreRepository.class));
    }
}
