/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RepositoriesServiceTests extends ESIntegTestCase {

    private final static String FAKE_FS_TYPE = "fakeFS";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestFSRepositoryPlugin.class);
    }

    public static class TestFSRepositoryPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry) {
            return Collections.singletonMap(FAKE_FS_TYPE,
                (metadata) -> new FsRepository(metadata, env, registry));
        }
    }

    public void testUpdateRepositoryWithDiffType() {
        String repositoryName = "test-repo";

        Client client = client();
        Settings settings = ((InternalTestCluster) cluster()).getDefaultSettings();
        Path location = randomRepoPath(settings);

        Settings.Builder repoSettings = Settings.builder().put(settings).put("location", location);

        PutRepositoryResponse putRepositoryResponse1 =
            client.admin().cluster().preparePutRepository(repositoryName)
                .setType(FsRepository.TYPE)
                .setSettings(repoSettings)
                .get();
        assertThat(putRepositoryResponse1.isAcknowledged(), equalTo(true));

        GetRepositoriesResponse getRepositoriesResponse1 =
            client.admin().cluster().prepareGetRepositories(repositoryName).get();

        assertThat(getRepositoriesResponse1.repositories(), hasSize(1));
        RepositoryMetaData repositoryMetaData1 = getRepositoriesResponse1.repositories().get(0);

        assertThat(repositoryMetaData1.type(), equalTo(FsRepository.TYPE));

        // update repository with different type

        PutRepositoryResponse putRepositoryResponse2 =
            client.admin().cluster().preparePutRepository(repositoryName)
                .setType(FAKE_FS_TYPE)
                .setSettings(repoSettings)
                .get();
        assertThat(putRepositoryResponse2.isAcknowledged(), equalTo(true));

        GetRepositoriesResponse getRepositoriesResponse2 =
            client.admin().cluster().prepareGetRepositories(repositoryName).get();

        assertThat(getRepositoriesResponse2.repositories(), hasSize(1));
        RepositoryMetaData repositoryMetaData2 = getRepositoriesResponse2.repositories().get(0);

        assertThat(repositoryMetaData2.type(), equalTo(FAKE_FS_TYPE));
    }
}
