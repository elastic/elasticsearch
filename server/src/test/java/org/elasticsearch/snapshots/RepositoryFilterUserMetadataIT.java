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

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class RepositoryFilterUserMetadataIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MetaDataFilteringPlugin.class);
    }

    public void testFilteredRepoMetaDataIsUsed() {
        final String masterName = internalCluster().getMasterName();
        final String repoName = "test-repo";
        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType(MetaDataFilteringPlugin.TYPE).setSettings(
            Settings.builder().put("location", randomRepoPath())
                .put(MetaDataFilteringPlugin.MASTER_SETTING_VALUE, masterName)));
        createIndex("test-idx");
        final SnapshotInfo snapshotInfo = client().admin().cluster().prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(true).get().getSnapshotInfo();
        assertThat(snapshotInfo.userMetadata(), is(Collections.singletonMap(MetaDataFilteringPlugin.MOCK_FILTERED_META, masterName)));
    }

    // Mock plugin that stores the name of the master node that started a snapshot in each snapshot's metadata
    public static final class MetaDataFilteringPlugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

        private static final String MOCK_FILTERED_META = "mock_filtered_meta";

        private static final String MASTER_SETTING_VALUE = "initial_master";

        private static final String TYPE = "mock_meta_filtering";

        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ClusterService clusterService) {
            return Collections.singletonMap("mock_meta_filtering", metadata ->
                new FsRepository(metadata, env, namedXContentRegistry, clusterService) {

                    // Storing the initially expected metadata value here to verify that #filterUserMetadata is only called once on the
                    // initial master node starting the snapshot
                    private final String initialMetaValue = metadata.settings().get(MASTER_SETTING_VALUE);

                    @Override
                    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                                 boolean writeShardGens, ActionListener<SnapshotInfo> listener) {
                        assertThat(userMetadata, is(Collections.singletonMap(MOCK_FILTERED_META, initialMetaValue)));
                        super.finalizeSnapshot(snapshotId, shardGenerations, startTime, failure, totalShards, shardFailures,
                            repositoryStateId, includeGlobalState, clusterMetaData, userMetadata, writeShardGens, listener);
                    }

                    @Override
                    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus,
                                              boolean writeShardGens, Map<String, Object> userMetadata, ActionListener<String> listener) {
                        assertThat(userMetadata, is(Collections.singletonMap(MOCK_FILTERED_META, initialMetaValue)));
                        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus,
                            writeShardGens, userMetadata, listener);
                    }

                    @Override
                    public Map<String, Object> adaptUserMetadata(Map<String, Object> userMetadata) {
                        return Collections.singletonMap(MOCK_FILTERED_META, clusterService.getNodeName());
                    }
                });
        }
    }
}
