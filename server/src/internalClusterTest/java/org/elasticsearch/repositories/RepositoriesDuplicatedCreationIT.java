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
import org.elasticsearch.common.Strings;
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
import static org.elasticsearch.test.hamcrest.ThrowableAssertions.assertThatException;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 3)
public class RepositoriesDuplicatedCreationIT extends ESIntegTestCase {
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

    public void testDuplicateCreateRepository() throws Exception {
        final String repositoryName = "test-duplicate-create-repo";
        String unstableNode = Strings.arrayToCommaDelimitedString(
            Arrays.stream(internalCluster().getNodeNames())
                .filter(name -> name.equals(internalCluster().getMasterName()) == false)
                .toArray()
        );

        // put repository for the first time: only let master node create repository successfully
        createRepository(
            repositoryName,
            UnstableRepository.TYPE,
            Settings.builder().put("location", randomRepoPath()).put(UnstableRepository.UNSTABLE_NODES.getKey(), unstableNode)
        );
        // verification should fail with some node has InvalidRepository
        try {
            client().admin().cluster().prepareVerifyRepository(repositoryName).get();
            fail("verification should fail when some node failed to create repository");
        } catch (Exception e) {
            assertThat(e, isA(RepositoryVerificationException.class));
            assertEquals(e.getSuppressed().length, 2);
            assertThatException(
                e.getSuppressed()[0].getCause(),
                RepositoryException.class,
                equalTo("[" + repositoryName + "] repository type [" + UnstableRepository.TYPE + "] failed to create on current node")
            );
            assertThatException(
                e.getSuppressed()[1].getCause(),
                RepositoryException.class,
                equalTo("[" + repositoryName + "] repository type [" + UnstableRepository.TYPE + "] failed to create on current node")
            );
        }

        // restart master
        internalCluster().restartNode(internalCluster().getMasterName());
        ensureGreen();

        // put repository again: let all node can create repository successfully
        createRepository(repositoryName, UnstableRepository.TYPE, Settings.builder().put("location", randomRepoPath()));
        // verification should succeed with all node create repository successfully
        VerifyRepositoryResponse verifyRepositoryResponse = client().admin().cluster().prepareVerifyRepository(repositoryName).get();
        assertEquals(verifyRepositoryResponse.getNodes().size(), 3);

    }

    private void createRepository(String name, String type, Settings.Builder settings) {
        // create
        assertAcked(client().admin().cluster().preparePutRepository(name).setType(type).setVerify(false).setSettings(settings).get());
        // get
        final GetRepositoriesResponse updatedGetRepositoriesResponse = client().admin().cluster().prepareGetRepositories(name).get();
        // assert
        assertThat(updatedGetRepositoriesResponse.repositories(), hasSize(1));
        final RepositoryMetadata updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);
        assertThat(updatedRepositoryMetadata.type(), equalTo(type));
    }
}
