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
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class RepositoriesServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MetaDataFilteringPlugin.class);
    }

    public void testFilteredRepoMetaDataIsUsed() {
        final String masterName = internalCluster().getMasterName();
        final String repoName = "test-repo";
        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType(MetaDataFilteringPlugin.TYPE).setSettings(
            Settings.builder().put("location", randomRepoPath())));
        // verify that the filtered setting has been initialized once on master and is the same on all nodes
        for (RepositoriesService instance : internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class)) {
            assertThat(instance.repository(repoName).getMetadata().settings().get(MetaDataFilteringPlugin.MOCK_FILTERED_SETTING),
                is(masterName));
        }
    }

    // Mock plugin that stores the name of the master node that created it initially in its metadata
    public static final class MetaDataFilteringPlugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

        private static final String MOCK_FILTERED_SETTING = "mock_filtered_setting";

        private static final String TYPE = "mock_meta_filtering";

        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ClusterService clusterService) {
            return Collections.singletonMap("mock_meta_filtering", metadata ->
                new FsRepository(filterRepoMetaData(clusterService, metadata), env, namedXContentRegistry, clusterService));
        }

        private static RepositoryMetaData filterRepoMetaData(ClusterService clusterService, RepositoryMetaData original) {
            final Settings settings = original.settings();
            if (settings.hasValue(MOCK_FILTERED_SETTING)) {
                return original;
            } else {
                assertThat("Only master may filter the setting", clusterService.state().nodes().isLocalNodeElectedMaster(), is(true));
                return new RepositoryMetaData(original.name(), original.type(),
                    Settings.builder().put(original.settings()).put(MOCK_FILTERED_SETTING, clusterService.getNodeName())
                        .build());
            }
        }
    }
}
