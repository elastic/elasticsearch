/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public final class EncryptedRepositorySecretIntegTests extends ESIntegTestCase {

    @BeforeClass
    public static void checkEnabled() {
        assumeFalse("Should only run when encrypted repo is enabled", EncryptedRepositoryPlugin.isDisabled());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateEncryptedRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.getTypeName())
            .build();
    }

    public void testRepositoryCreationFailsForMissingPassword() throws Exception {
        // if the password is missing on the master node, the repository creation fails
        final String repositoryName = randomName();
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            randomAlphaOfLength(20)
        );
        logger.info("--> start 3 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNodeName = internalCluster().startNode();
        logger.info("--> started master node " + masterNodeName);
        ensureStableCluster(1);
        internalCluster().startNodes(2, Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        logger.info("--> started two other nodes");
        ensureStableCluster(3);
        assertThat(masterNodeName, equalTo(internalCluster().getMasterName()));

        final Settings repositorySettings = repositorySettings(repositoryName);
        RepositoryException e = expectThrows(
            RepositoryException.class,
            () -> client().admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType(repositoryType())
                .setVerify(randomBoolean())
                .setSettings(repositorySettings)
                .get()
        );
        assertThat(e.getMessage(), containsString("failed to create repository"));
        expectThrows(RepositoryMissingException.class, () -> client().admin().cluster().prepareGetRepositories(repositoryName).get());

        if (randomBoolean()) {
            // stop the node with the missing password
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodeName));
            ensureStableCluster(2);
        } else {
            // restart the node with the missing password
            internalCluster().restartNode(masterNodeName, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    Settings.Builder newSettings = Settings.builder().put(super.onNodeStopped(nodeName));
                    newSettings.setSecureSettings(secureSettingsWithPassword);
                    return newSettings.build();
                }
            });
            ensureStableCluster(3);
        }
        // repository creation now successful
        createRepository(repositoryName, repositorySettings, true);
    }

    public void testRepositoryVerificationFailsForMissingPassword() throws Exception {
        // if the password is missing on any non-master node, the repository verification fails
        final String repositoryName = randomName();
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            randomAlphaOfLength(20)
        );
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNodeName = internalCluster().startNode(Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        logger.info("--> started master node " + masterNodeName);
        ensureStableCluster(1);
        final String otherNodeName = internalCluster().startNode();
        logger.info("--> started other node " + otherNodeName);
        ensureStableCluster(2);
        assertThat(masterNodeName, equalTo(internalCluster().getMasterName()));
        // repository create fails verification
        final Settings repositorySettings = repositorySettings(repositoryName);
        expectThrows(
            RepositoryVerificationException.class,
            () -> client().admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType(repositoryType())
                .setVerify(true)
                .setSettings(repositorySettings)
                .get()
        );
        if (randomBoolean()) {
            // delete and recreate repo
            logger.debug("-->  deleting repository [name: {}]", repositoryName);
            assertAcked(client().admin().cluster().prepareDeleteRepository(repositoryName));
            assertAcked(
                client().admin()
                    .cluster()
                    .preparePutRepository(repositoryName)
                    .setType(repositoryType())
                    .setVerify(false)
                    .setSettings(repositorySettings)
                    .get()
            );
        }
        // test verify call fails
        expectThrows(RepositoryVerificationException.class, () -> client().admin().cluster().prepareVerifyRepository(repositoryName).get());
        if (randomBoolean()) {
            // stop the node with the missing password
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(otherNodeName));
            ensureStableCluster(1);
            // repository verification now succeeds
            VerifyRepositoryResponse verifyRepositoryResponse = client().admin().cluster().prepareVerifyRepository(repositoryName).get();
            List<String> verifiedNodes = verifyRepositoryResponse.getNodes().stream().map(n -> n.getName()).collect(Collectors.toList());
            assertThat(verifiedNodes, contains(masterNodeName));
        } else {
            // restart the node with the missing password
            internalCluster().restartNode(otherNodeName, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    Settings.Builder newSettings = Settings.builder().put(super.onNodeStopped(nodeName));
                    newSettings.setSecureSettings(secureSettingsWithPassword);
                    return newSettings.build();
                }
            });
            ensureStableCluster(2);
            // repository verification now succeeds
            VerifyRepositoryResponse verifyRepositoryResponse = client().admin().cluster().prepareVerifyRepository(repositoryName).get();
            List<String> verifiedNodes = verifyRepositoryResponse.getNodes().stream().map(n -> n.getName()).collect(Collectors.toList());
            assertThat(verifiedNodes, containsInAnyOrder(masterNodeName, otherNodeName));
        }
    }

    public void testRepositoryVerificationFailsForDifferentPassword() throws Exception {
        final String repositoryName = randomName();
        final String repoPass1 = randomAlphaOfLength(20);
        final String repoPass2 = randomAlphaOfLength(19);
        // put a different repository password
        MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            repoPass1
        );
        MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            repoPass2
        );
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(1);
        final String node1 = internalCluster().startNode(Settings.builder().setSecureSettings(secureSettings1).build());
        final String node2 = internalCluster().startNode(Settings.builder().setSecureSettings(secureSettings2).build());
        ensureStableCluster(2);
        // repository create fails verification
        Settings repositorySettings = repositorySettings(repositoryName);
        expectThrows(
            RepositoryVerificationException.class,
            () -> client().admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType(repositoryType())
                .setVerify(true)
                .setSettings(repositorySettings)
                .get()
        );
        if (randomBoolean()) {
            // delete and recreate repo
            logger.debug("-->  deleting repository [name: {}]", repositoryName);
            assertAcked(client().admin().cluster().prepareDeleteRepository(repositoryName));
            assertAcked(
                client().admin()
                    .cluster()
                    .preparePutRepository(repositoryName)
                    .setType(repositoryType())
                    .setVerify(false)
                    .setSettings(repositorySettings)
                    .get()
            );
        }
        // test verify call fails
        expectThrows(RepositoryVerificationException.class, () -> client().admin().cluster().prepareVerifyRepository(repositoryName).get());
        // restart one of the nodes to use the same password
        if (randomBoolean()) {
            secureSettings1.setString(
                EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
                repoPass2
            );
            internalCluster().restartNode(node1, new InternalTestCluster.RestartCallback());
        } else {
            secureSettings2.setString(
                EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
                repoPass1
            );
            internalCluster().restartNode(node2, new InternalTestCluster.RestartCallback());
        }
        ensureStableCluster(2);
        // repository verification now succeeds
        VerifyRepositoryResponse verifyRepositoryResponse = client().admin().cluster().prepareVerifyRepository(repositoryName).get();
        List<String> verifiedNodes = verifyRepositoryResponse.getNodes().stream().map(n -> n.getName()).collect(Collectors.toList());
        assertThat(verifiedNodes, containsInAnyOrder(node1, node2));
    }

    public void testLicenseComplianceSnapshotAndRestore() throws Exception {
        final String repositoryName = randomName();
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            randomAlphaOfLength(20)
        );
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(1);
        internalCluster().startNodes(2, Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        ensureStableCluster(2);

        logger.info("--> creating repo " + repositoryName);
        createRepository(repositoryName);
        final String indexName = randomName();
        logger.info("-->  create random index {} with {} records", indexName, 3);
        indexRandom(
            true,
            client().prepareIndex(indexName).setId("1").setSource("field1", "the quick brown fox jumps"),
            client().prepareIndex(indexName).setId("2").setSource("field1", "quick brown"),
            client().prepareIndex(indexName).setId("3").setSource("field1", "quick")
        );
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 3);

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repositoryName, snapshotName);
        assertSuccessfulSnapshot(
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repositoryName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .get()
        );

        // make license not accept encrypted snapshots
        EncryptedRepository encryptedRepository = (EncryptedRepository) internalCluster().getCurrentMasterNodeInstance(
            RepositoriesService.class
        ).repository(repositoryName);
        encryptedRepository.licenseStateSupplier = () -> {
            XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
            when(mockLicenseState.isAllowed(anyObject())).thenReturn(false);
            return mockLicenseState;
        };

        // now snapshot is not permitted
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName + "2").setWaitForCompletion(true).get()
        );
        assertThat(e.getDetailedMessage(), containsString("current license is non-compliant for [encrypted snapshots]"));

        logger.info("-->  delete index {}", indexName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // but restore is permitted
        logger.info("--> restore index from the snapshot");
        assertSuccessfulRestore(
            client().admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotName).setWaitForCompletion(true).get()
        );
        ensureGreen();
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 3);
        // also delete snapshot is permitted
        logger.info("-->  delete snapshot {}:{}", repositoryName, snapshotName);
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotName).get());
    }

    public void testSnapshotIsPartialForMissingPassword() throws Exception {
        final String repositoryName = randomName();
        final Settings repositorySettings = repositorySettings(repositoryName);
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            randomAlphaOfLength(20)
        );
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        // master has the password
        internalCluster().startNode(Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        ensureStableCluster(1);
        final String otherNode = internalCluster().startNode();
        ensureStableCluster(2);
        logger.debug("-->  creating repository [name: {}, verify: {}, settings: {}]", repositoryName, false, repositorySettings);
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType(repositoryType())
                .setVerify(false)
                .setSettings(repositorySettings)
        );
        // create an index with the shard on the node without a repository password
        final String indexName = randomName();
        final Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put("index.routing.allocation.include._name", otherNode)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        logger.info("-->  create random index {}", indexName);
        createIndex(indexName, indexSettings);
        indexRandom(
            true,
            client().prepareIndex(indexName).setId("1").setSource("field1", "the quick brown fox jumps"),
            client().prepareIndex(indexName).setId("2").setSource("field1", "quick brown"),
            client().prepareIndex(indexName).setId("3").setSource("field1", "quick")
        );
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 3);

        // empty snapshot completes successfully because it does not involve data on the node without a repository password
        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repositoryName, snapshotName);
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setIndices(indexName + "other*")
            .setWaitForCompletion(true)
            .get();
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_HASH_USER_METADATA_KEY))
        );
        assertThat(
            createSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_SALT_USER_METADATA_KEY))
        );

        // snapshot is PARTIAL because it includes shards on nodes with a missing repository password
        final String snapshotName2 = snapshotName + "2";
        CreateSnapshotResponse incompleteSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName2)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(incompleteSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        assertTrue(
            incompleteSnapshotResponse.getSnapshotInfo()
                .shardFailures()
                .stream()
                .allMatch(shardFailure -> shardFailure.reason().contains("[" + repositoryName + "] missing"))
        );
        assertThat(
            incompleteSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_HASH_USER_METADATA_KEY))
        );
        assertThat(
            incompleteSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_SALT_USER_METADATA_KEY))
        );
        final Set<String> nodesWithFailures = incompleteSnapshotResponse.getSnapshotInfo()
            .shardFailures()
            .stream()
            .map(sf -> sf.nodeId())
            .collect(Collectors.toSet());
        assertThat(nodesWithFailures.size(), equalTo(1));
        final ClusterStateResponse clusterState = client().admin().cluster().prepareState().clear().setNodes(true).get();
        assertThat(clusterState.getState().nodes().get(nodesWithFailures.iterator().next()).getName(), equalTo(otherNode));
    }

    public void testSnapshotIsPartialForDifferentPassword() throws Exception {
        final String repoName = randomName();
        final Settings repoSettings = repositorySettings(repoName);
        final String repoPass1 = randomAlphaOfLength(20);
        final String repoPass2 = randomAlphaOfLength(19);
        MockSecureSettings secureSettingsMaster = new MockSecureSettings();
        secureSettingsMaster.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repoName).getKey(),
            repoPass1
        );
        MockSecureSettings secureSettingsOther = new MockSecureSettings();
        secureSettingsOther.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repoName).getKey(),
            repoPass2
        );
        final boolean putRepoEarly = randomBoolean();
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startNode(Settings.builder().setSecureSettings(secureSettingsMaster).build());
        ensureStableCluster(1);
        if (putRepoEarly) {
            createRepository(repoName, repoSettings, true);
        }
        final String otherNode = internalCluster().startNode(Settings.builder().setSecureSettings(secureSettingsOther).build());
        ensureStableCluster(2);
        if (false == putRepoEarly) {
            createRepository(repoName, repoSettings, false);
        }

        // create index with shards on both nodes
        final String indexName = randomName();
        final Settings indexSettings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 5).build();
        logger.info("-->  create random index {}", indexName);
        createIndex(indexName, indexSettings);
        indexRandom(
            true,
            client().prepareIndex(indexName).setId("1").setSource("field1", "the quick brown fox jumps"),
            client().prepareIndex(indexName).setId("2").setSource("field1", "quick brown"),
            client().prepareIndex(indexName).setId("3").setSource("field1", "quick"),
            client().prepareIndex(indexName).setId("4").setSource("field1", "lazy"),
            client().prepareIndex(indexName).setId("5").setSource("field1", "dog")
        );
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 5);

        // empty snapshot completes successfully for both repos because it does not involve any data
        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setIndices(indexName + "other*")
            .setWaitForCompletion(true)
            .get();
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_HASH_USER_METADATA_KEY))
        );
        assertThat(
            createSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_SALT_USER_METADATA_KEY))
        );

        // snapshot is PARTIAL because it includes shards on nodes with a different repository KEK
        final String snapshotName2 = snapshotName + "2";
        CreateSnapshotResponse incompleteSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName2)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(incompleteSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        assertTrue(
            incompleteSnapshotResponse.getSnapshotInfo()
                .shardFailures()
                .stream()
                .allMatch(shardFailure -> shardFailure.reason().contains("Repository password mismatch"))
        );
        final Set<String> nodesWithFailures = incompleteSnapshotResponse.getSnapshotInfo()
            .shardFailures()
            .stream()
            .map(sf -> sf.nodeId())
            .collect(Collectors.toSet());
        assertThat(nodesWithFailures.size(), equalTo(1));
        final ClusterStateResponse clusterState = client().admin().cluster().prepareState().clear().setNodes(true).get();
        assertThat(clusterState.getState().nodes().get(nodesWithFailures.iterator().next()).getName(), equalTo(otherNode));
    }

    public void testWrongRepositoryPassword() throws Exception {
        final String repositoryName = randomName();
        final Settings repositorySettings = repositorySettings(repositoryName);
        final String goodPassword = randomAlphaOfLength(20);
        final String wrongPassword = randomAlphaOfLength(19);
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            goodPassword
        );
        logger.info("--> start 2 nodes");
        internalCluster().setBootstrapMasterNodeIndex(1);
        internalCluster().startNodes(2, Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        ensureStableCluster(2);
        createRepository(repositoryName, repositorySettings, true);
        final String snapshotName = randomName();
        logger.info("-->  create empty snapshot {}:{}", repositoryName, snapshotName);
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_HASH_USER_METADATA_KEY))
        );
        assertThat(
            createSnapshotResponse.getSnapshotInfo().userMetadata(),
            not(hasKey(EncryptedRepository.PASSWORD_SALT_USER_METADATA_KEY))
        );
        // restart master node and fill in a wrong password
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            wrongPassword
        );

        internalCluster().fullRestart();
        ensureGreen();
        // maybe recreate the repository
        if (randomBoolean()) {
            deleteRepository(repositoryName);
            createRepository(repositoryName, repositorySettings, false);
        }
        // all repository operations return "repository password is incorrect", but the repository does not move to the corrupted state
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(
            RepositoriesService.class
        ).repository(repositoryName);
        RepositoryException e = expectThrows(
            RepositoryException.class,
            () -> PlainActionFuture.<RepositoryData, Exception>get(blobStoreRepository::getRepositoryData)
        );
        assertThat(e.getCause().getMessage(), containsString("repository password is incorrect"));
        e = expectThrows(
            RepositoryException.class,
            () -> client().admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName + "2").setWaitForCompletion(true).get()
        );
        assertThat(e.getCause().getMessage(), containsString("repository password is incorrect"));
        e = expectThrows(RepositoryException.class, () -> client().admin().cluster().prepareGetSnapshots(repositoryName).get());
        assertThat(e.getCause().getMessage(), containsString("repository password is incorrect"));
        e = expectThrows(
            RepositoryException.class,
            () -> client().admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotName).setWaitForCompletion(true).get()
        );
        assertThat(e.getCause().getMessage(), containsString("repository password is incorrect"));
        e = expectThrows(
            RepositoryException.class,
            () -> client().admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotName).get()
        );
        assertThat(e.getCause().getMessage(), containsString("repository password is incorrect"));
        // restart master node and fill in the good password
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repositoryName).getKey(),
            goodPassword
        );
        internalCluster().fullRestart();
        ensureGreen();
        // ensure get snapshot works
        GetSnapshotsResponse getSnapshotResponse = client().admin().cluster().prepareGetSnapshots(repositoryName).get();
        assertThat(getSnapshotResponse.getSnapshots(), hasSize(1));
    }

    public void testSnapshotFailsForMasterFailoverWithWrongPassword() throws Exception {
        final String repoName = randomName();
        final Settings repoSettings = repositorySettings(repoName);
        final String goodPass = randomAlphaOfLength(20);
        final String wrongPass = randomAlphaOfLength(19);
        MockSecureSettings secureSettingsWithPassword = new MockSecureSettings();
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repoName).getKey(),
            goodPass
        );
        logger.info("--> start 4 nodes");
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNodes(
            1,
            Settings.builder().setSecureSettings(secureSettingsWithPassword).build()
        ).get(0);
        final String otherNode = internalCluster().startDataOnlyNodes(
            1,
            Settings.builder().setSecureSettings(secureSettingsWithPassword).build()
        ).get(0);
        ensureStableCluster(2);
        secureSettingsWithPassword.setString(
            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(repoName).getKey(),
            wrongPass
        );
        internalCluster().startMasterOnlyNodes(2, Settings.builder().setSecureSettings(secureSettingsWithPassword).build());
        ensureStableCluster(4);
        assertThat(internalCluster().getMasterName(), equalTo(masterNode));

        logger.debug("-->  creating repository [name: {}, verify: {}, settings: {}]", repoName, false, repoSettings);
        assertAcked(
            client().admin().cluster().preparePutRepository(repoName).setType(repositoryType()).setVerify(false).setSettings(repoSettings)
        );
        // create index with just one shard on the "other" data node
        final String indexName = randomName();
        final Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put("index.routing.allocation.include._name", otherNode)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        logger.info("-->  create random index {}", indexName);
        createIndex(indexName, indexSettings);
        indexRandom(
            true,
            client().prepareIndex(indexName).setId("1").setSource("field1", "the quick brown fox jumps"),
            client().prepareIndex(indexName).setId("2").setSource("field1", "quick brown"),
            client().prepareIndex(indexName).setId("3").setSource("field1", "quick"),
            client().prepareIndex(indexName).setId("4").setSource("field1", "lazy"),
            client().prepareIndex(indexName).setId("5").setSource("field1", "dog")
        );
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 5);

        // block shard snapshot on the data node
        final LocalStateEncryptedRepositoryPlugin.TestEncryptedRepository otherNodeEncryptedRepo =
            (LocalStateEncryptedRepositoryPlugin.TestEncryptedRepository) internalCluster().getInstance(
                RepositoriesService.class,
                otherNode
            ).repository(repoName);
        otherNodeEncryptedRepo.blockSnapshotShard();

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setIndices(indexName).setWaitForCompletion(false).get();

        // stop master
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNode));
        ensureStableCluster(3);

        otherNodeEncryptedRepo.unblockSnapshotShard();

        // the failover master has the wrong password, snapshot fails
        logger.info("--> waiting for completion");
        expectThrows(SnapshotMissingException.class, () -> { waitForCompletion(repoName, snapshotName, TimeValue.timeValueSeconds(60)); });
    }

    protected String randomName() {
        return randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    }

    protected String repositoryType() {
        return EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME;
    }

    protected Settings repositorySettings(String repositoryName) {
        return Settings.builder()
            .put("compress", randomBoolean())
            .put(EncryptedRepositoryPlugin.DELEGATE_TYPE_SETTING.getKey(), FsRepository.TYPE)
            .put(EncryptedRepositoryPlugin.PASSWORD_NAME_SETTING.getKey(), repositoryName)
            .put("location", randomRepoPath())
            .build();
    }

    protected String createRepository(final String name) {
        return createRepository(name, true);
    }

    protected String createRepository(final String name, final boolean verify) {
        return createRepository(name, repositorySettings(name), verify);
    }

    protected String createRepository(final String name, final Settings settings, final boolean verify) {
        logger.debug("-->  creating repository [name: {}, verify: {}, settings: {}]", name, verify, settings);
        assertAcked(
            client().admin().cluster().preparePutRepository(name).setType(repositoryType()).setVerify(verify).setSettings(settings)
        );

        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            assertThat(repositories.repository(name), notNullValue());
            assertThat(repositories.repository(name), instanceOf(BlobStoreRepository.class));
            assertThat(repositories.repository(name).isReadOnly(), is(settings.getAsBoolean(READONLY_SETTING_KEY, false)));
        });

        return name;
    }

    protected void deleteRepository(final String name) {
        logger.debug("-->  deleting repository [name: {}]", name);
        assertAcked(client().admin().cluster().prepareDeleteRepository(name));

        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            RepositoryMissingException e = expectThrows(RepositoryMissingException.class, () -> repositories.repository(name));
            assertThat(e.repository(), equalTo(name));
        });
    }

    private void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }

    private void assertSuccessfulSnapshot(CreateSnapshotResponse response) {
        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
        assertThat(response.getSnapshotInfo().userMetadata(), not(hasKey(EncryptedRepository.PASSWORD_HASH_USER_METADATA_KEY)));
        assertThat(response.getSnapshotInfo().userMetadata(), not(hasKey(EncryptedRepository.PASSWORD_SALT_USER_METADATA_KEY)));
    }

    public SnapshotInfo waitForCompletion(String repository, String snapshotName, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.millis()) {
            List<SnapshotInfo> snapshotInfos = client().admin()
                .cluster()
                .prepareGetSnapshots(repository)
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                // Make sure that snapshot clean up operations are finished
                ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
                SnapshotsInProgress snapshotsInProgress = stateResponse.getState().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress == null) {
                    return snapshotInfos.get(0);
                } else {
                    boolean found = false;
                    for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                        final Snapshot curr = entry.snapshot();
                        if (curr.getRepository().equals(repository) && curr.getSnapshotId().getName().equals(snapshotName)) {
                            found = true;
                            break;
                        }
                    }
                    if (found == false) {
                        return snapshotInfos.get(0);
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }
}
