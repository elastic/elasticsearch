/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public abstract class AbstractArchiveTestCase extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateOldLuceneVersions.class, TestRepositoryPlugin.class, MockRepository.Plugin.class);
    }

    public static class TestRepositoryPlugin extends Plugin implements RepositoryPlugin {
        public static final String FAKE_VERSIONS_TYPE = "fakeversionsrepo";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Map.of(
                FAKE_VERSIONS_TYPE,
                metadata -> new FakeVersionsRepo(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }

        // fakes an old index version format to activate license checks
        private static class FakeVersionsRepo extends FsRepository {
            FakeVersionsRepo(
                RepositoryMetadata metadata,
                Environment env,
                NamedXContentRegistry namedXContentRegistry,
                ClusterService clusterService,
                BigArrays bigArrays,
                RecoverySettings recoverySettings
            ) {
                super(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            }

            @Override
            public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index)
                throws IOException {
                final IndexMetadata original = super.getSnapshotIndexMetaData(repositoryData, snapshotId, index);
                return IndexMetadata.builder(original)
                    .settings(
                        Settings.builder()
                            .put(original.getSettings())
                            .put(
                                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                                metadata.settings()
                                    .getAsVersionId("version", IndexVersion::fromId, IndexVersion.fromId(randomFrom(5000099, 6000099)))
                            )
                    )
                    .build();
            }
        }
    }

    protected static final String repoName = "test-repo";
    protected static final String indexName = "test-index";
    protected static final String snapshotName = "test-snapshot";

    @Before
    public void createAndRestoreArchive() throws Exception {
        createRepository(repoName, TestRepositoryPlugin.FAKE_VERSIONS_TYPE);
        createIndex(indexName);
        createFullSnapshot(repoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        PostStartTrialRequest request = new PostStartTrialRequest(TEST_REQUEST_TIMEOUT).setType(License.LicenseType.TRIAL.getTypeName())
            .acknowledge(true);
        client().execute(PostStartTrialAction.INSTANCE, request).get();
    }
}
