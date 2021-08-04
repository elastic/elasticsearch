/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class RepositoriesServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockRepository.Plugin.class);
    }

    public void testUpdateRepository() {
        final InternalTestCluster cluster = internalCluster();

        final String repositoryName = "test-repo";

        final Client client = client();
        final RepositoriesService repositoriesService = cluster.getDataOrMasterNodeInstances(RepositoriesService.class).iterator().next();

        final Settings.Builder repoSettings = Settings.builder().put("location", randomRepoPath());

        assertAcked(
            client.admin().cluster().preparePutRepository(repositoryName).setType(FsRepository.TYPE).setSettings(repoSettings).get()
        );

        final GetRepositoriesResponse originalGetRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(repositoryName)
            .get();

        assertThat(originalGetRepositoriesResponse.repositories(), hasSize(1));
        RepositoryMetadata originalRepositoryMetadata = originalGetRepositoriesResponse.repositories().get(0);

        assertThat(originalRepositoryMetadata.type(), equalTo(FsRepository.TYPE));

        final Repository originalRepository = repositoriesService.repository(repositoryName);
        assertThat(originalRepository, instanceOf(FsRepository.class));

        final boolean updated = randomBoolean();
        final String updatedRepositoryType = updated ? "mock" : FsRepository.TYPE;

        assertAcked(
            client.admin().cluster().preparePutRepository(repositoryName).setType(updatedRepositoryType).setSettings(repoSettings).get()
        );

        final GetRepositoriesResponse updatedGetRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(repositoryName)
            .get();

        assertThat(updatedGetRepositoriesResponse.repositories(), hasSize(1));
        final RepositoryMetadata updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);

        assertThat(updatedRepositoryMetadata.type(), equalTo(updatedRepositoryType));

        final Repository updatedRepository = repositoriesService.repository(repositoryName);
        assertThat(updatedRepository, updated ? not(sameInstance(originalRepository)) : sameInstance(originalRepository));

        // check that a noop update does not verify.
        internalCluster().startDataOnlyNode(Settings.builder().put(Environment.PATH_REPO_SETTING.getKey(), createTempDir()).build());
        assertAcked(
            client.admin().cluster().preparePutRepository(repositoryName).setType(updatedRepositoryType).setSettings(repoSettings).get()
        );
    }
}
