/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

public class InvalidRepositoryIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(UnstableRepository.Plugin.class);
    }

    public static class UnstableRepository extends MockRepository {
        public static final String TYPE = "unstable";
        public static final Setting<List<String>> UNSTABLE_NODES = Setting.stringListSetting(
            "repository.unstable_nodes",
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

        public UnstableRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            List<String> unstableNodes = UNSTABLE_NODES.get(metadata.settings());
            if (unstableNodes.contains(clusterService.getNodeName())) {
                throw new RepositoryException(metadata.name(), "Failed to create repository: current node is not stable");
            }
        }

        public static class Plugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {
            @Override
            public Map<String, Factory> getRepositories(
                Environment env,
                NamedXContentRegistry namedXContentRegistry,
                ClusterService clusterService,
                BigArrays bigArrays,
                RecoverySettings recoverySettings
            ) {
                return Collections.singletonMap(
                    TYPE,
                    (metadata) -> new UnstableRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
                );
            }

            @Override
            public List<Setting<?>> getSettings() {
                return List.of(UNSTABLE_NODES);
            }
        }
    }

    public void testCreateInvalidRepository() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String repositoryName = "test-duplicate-create-repo";

        // put repository for the first time: only let master node create repository successfully
        createRepository(
            repositoryName,
            UnstableRepository.TYPE,
            Settings.builder()
                .put("location", randomRepoPath())
                .putList(
                    UnstableRepository.UNSTABLE_NODES.getKey(),
                    Arrays.stream(internalCluster().getNodeNames())
                        .filter(name -> name.equals(internalCluster().getMasterName()) == false)
                        .toList()
                )
        );
        // verification should fail with some node has InvalidRepository
        final var expectedException = expectThrows(
            RepositoryVerificationException.class,
            () -> clusterAdmin().prepareVerifyRepository(repositoryName).get()
        );
        for (Throwable suppressed : expectedException.getSuppressed()) {
            Throwable outerCause = suppressed.getCause();
            assertThat(outerCause, isA(RepositoryException.class));
            assertThat(
                outerCause.getMessage(),
                equalTo("[" + repositoryName + "] repository type [" + UnstableRepository.TYPE + "] failed to create on current node")
            );
            Throwable innerCause = suppressed.getCause().getCause().getCause();
            assertThat(innerCause, isA(RepositoryException.class));
            assertThat(
                innerCause.getMessage(),
                equalTo("[" + repositoryName + "] Failed to create repository: current node is not stable")
            );
        }

        // restart master
        internalCluster().restartNode(internalCluster().getMasterName());
        ensureGreen();

        // put repository again: let all node can create repository successfully
        createRepository(repositoryName, UnstableRepository.TYPE, Settings.builder().put("location", randomRepoPath()));
        // verification should succeed with all node create repository successfully
        VerifyRepositoryResponse verifyRepositoryResponse = clusterAdmin().prepareVerifyRepository(repositoryName).get();
        assertEquals(verifyRepositoryResponse.getNodes().size(), internalCluster().numDataAndMasterNodes());

    }

    private void createRepository(String name, String type, Settings.Builder settings) {
        // create
        assertAcked(clusterAdmin().preparePutRepository(name).setType(type).setVerify(false).setSettings(settings).get());
        // get
        final GetRepositoriesResponse updatedGetRepositoriesResponse = clusterAdmin().prepareGetRepositories(name).get();
        // assert
        assertThat(updatedGetRepositoriesResponse.repositories(), hasSize(1));
        final RepositoryMetadata updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);
        assertThat(updatedRepositoryMetadata.type(), equalTo(type));
    }
}
