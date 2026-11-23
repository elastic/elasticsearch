/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.JoinValidationService;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.settings.secure.SecureSettingsPlugin;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.encryption.EncryptionIntegTestUtils.passwordSettings;
import static org.elasticsearch.xpack.encryption.ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY;
import static org.elasticsearch.xpack.encryption.ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * On stateless only master-eligible nodes persist cluster state, so only they must be able to wrap the cluster state encryption key.
 * When the password comes from cluster secrets every node gets it from the cluster state, so nodes are not checked for it; otherwise each
 * node uses the password in its own keystore, which may differ between nodes.
 *
 * <p>Nodes start without a password. Keystore passwords are added with a secure settings reload, so that a restart drops them.
 */
@LuceneTestCase.SuppressFileSystems("*")
// not auto-managed, so that starting a master-eligible node that is rejected at join doesn't wait for it to join
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class StatelessEncryptionPasswordIT extends AbstractStatelessPluginIntegTestCase {

    private static final Settings FAST_FAILOVER = Settings.builder()
        .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
        .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
        .build();

    @Before
    public void bootstrapFirstMaster() {
        internalCluster().setBootstrapMasterNodeIndex(0);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        plugins.add(SecureSettingsPlugin.class);
        return plugins;
    }

    /** A master-eligible node without a password at startup joins, since it gets the password from cluster secrets once it has joined. */
    public void testNodeWithoutPasswordJoinsWhenPasswordIsInClusterSecrets() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        ensureStableCluster(3);
        applyClusterSecrets(1, ACTIVE_PASSWORD_ID_KEY, "v1", PASSWORD_PREFIX + "v1", "test-password-v1");
        assertBusy(() -> assertTrue(hasUsableKey(masterState())));

        String joining = startMasterOnlyNode();
        ensureStableCluster(4);
        assertBusy(() -> assertTrue(pekEncryption(joining).canWrapForGateway()));
        assertThat(pekEncryption(joining).activePasswordId(), equalTo("v1"));
        assertTrue(hasUsableKey(masterState()));

        createIndex("index-after-join");
        ensureGreen("index-after-join");
    }

    /**
     * Cluster secrets that switch to a new password and drop the old one are applied on every node, although the elected master's
     * persisted copy of the key is still wrapped with the old one: cluster secrets are the source of truth.
     */
    public void testClusterSecretsReplacingThePasswordAreAppliedOnEveryNode() throws Exception {
        startMasterOnlyNode();
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        ensureStableCluster(4);
        applyClusterSecrets(1, ACTIVE_PASSWORD_ID_KEY, "v1", PASSWORD_PREFIX + "v1", "test-password-v1");
        assertBusy(() -> assertThat(masterPek().getPasswordId(), equalTo("v1")));

        applyClusterSecrets(2, ACTIVE_PASSWORD_ID_KEY, "v2", PASSWORD_PREFIX + "v2", "test-password-v2");
        for (String node : internalCluster().getNodeNames()) {
            assertThat(node, pekEncryption(node).activePasswordId(), equalTo("v2"));
            assertTrue(node, pekEncryption(node).canWrapForGateway());
        }
        assertTrue(hasUsableKey(masterState()));
        // the key metadata follows the new password although the old one is gone
        assertBusy(() -> assertThat(masterPek().getPasswordId(), equalTo("v2")));

        createIndex("index-after-password-change");
        ensureGreen("index-after-password-change");
    }

    /**
     * With keystore passwords, a master-eligible node without one can't join once the key is installed, while index and search nodes,
     * which don't persist cluster state, need none. A master-eligible node with a password of its own joins, and when it takes over from
     * a running master it persists the key it already holds with that password: it doesn't read the copy the previous master wrapped.
     */
    public void testMasterEligibleNodeWithoutKeystorePasswordCannotJoin() throws Exception {
        // otherwise the join is validated against the cached state from the index node's join, which has no key yet
        String master = startMasterOnlyNode(
            Settings.builder()
                .put(FAST_FAILOVER)
                .put(JoinValidationService.JOIN_VALIDATION_CACHE_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(1))
                .build()
        );
        String indexNode = startIndexNode();
        ensureStableCluster(2);
        // the index node has no password, and the key is installed anyway
        reload(master, passwordSettings("v1", "test-password-v1"));
        assertBusy(() -> assertTrue(hasUsableKey(masterState())));

        String searchNode = startSearchNode();
        String secondIndexNode = startIndexNode();
        ensureStableCluster(4);

        String joining;
        try (MockLog mockLog = MockLog.capture(JoinHelper.class)) {
            mockLog.addExpectation(new EncryptionIntegTestUtils.JoinRejectedExpectation());
            // a single late heartbeat would let it take over before it has joined, reading the copy the master wrapped with its password
            joining = startMasterOnlyNode(
                Settings.builder().put(FAST_FAILOVER).put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 5).build()
            );
            mockLog.awaitAllExpectationsMatched();
        }
        // asked on the master: a random node may be the rejected one, which has no master
        assertEquals(4, internalCluster().clusterService(master).state().nodes().size());

        // the node retries joining, and is admitted once it can persist the key, with a password of its own
        reload(joining, passwordSettings("v2", "test-password-v2"));
        ensureStableCluster(5);
        assertTrue(hasUsableKey(masterState()));

        internalCluster().stopNode(master);
        assertBusy(() -> {
            ClusterState state = internalCluster().clusterService(joining).state();
            assertTrue(state.nodes().isLocalNodeElectedMaster());
            assertTrue(hasUsableKey(state));
        }, 30, TimeUnit.SECONDS);
        createIndex("index-after-failover");
        ensureGreen("index-after-failover");

        // stopped because the test framework's teardown writes cluster state as if to disk on a random node
        for (String node : List.of(indexNode, secondIndexNode, searchNode)) {
            internalCluster().stopNode(node);
        }
    }

    /** The elected master restarts without its password, so it holds no key; it restores the key once its password is reloaded. */
    public void testRestartedMasterRestoresKeyOnceItHasThePassword() throws Exception {
        String master = startMasterOnlyNode(FAST_FAILOVER);
        reload(master, passwordSettings("v1", "test-password-v1"));
        assertBusy(() -> assertTrue(hasUsableKey(masterState())));

        internalCluster().restartNode(master);
        assertBusy(() -> assertTrue(masterPek().isUnwrapFailed()));

        reload(master, passwordSettings("v1", "test-password-v1"));
        assertBusy(() -> assertTrue(hasUsableKey(masterState())));
    }

    /** Writes file settings holding the given cluster secrets, as alternating names and values, and waits for every node to apply them. */
    private static void applyClusterSecrets(long version, String... namesAndValues) throws Exception {
        FileSettingsService fileSettings = internalCluster().getInstance(FileSettingsService.class, internalCluster().getMasterName());
        assertBusy(() -> assertTrue(fileSettings.watching()));
        Files.createDirectories(fileSettings.watchedFileDir());
        Path tempFile = createTempFile();
        Files.writeString(tempFile, EncryptionIntegTestUtils.clusterSecretsJson(version, namesAndValues));
        Files.move(tempFile, fileSettings.watchedFile(), StandardCopyOption.ATOMIC_MOVE);

        assertBusy(() -> {
            ReservedStateMetadata reserved = masterState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
            assertNotNull(reserved);
            assertEquals(version, reserved.version().longValue());
        });
        // every node has applied the cluster secrets, and so reloaded them, once the master has no pending tasks
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get();
    }

    private static ClusterState masterState() {
        return internalCluster().clusterService(internalCluster().getMasterName()).state();
    }

    private static ProjectEncryptionKeyMetadata pek(ClusterState state) {
        return state.metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private static ProjectEncryptionKeyMetadata masterPek() {
        ProjectEncryptionKeyMetadata metadata = pek(masterState());
        assertThat(metadata, notNullValue());
        return metadata;
    }

    private static boolean hasUsableKey(ClusterState state) {
        ProjectEncryptionKeyMetadata metadata = pek(state);
        return metadata != null && metadata.isUnwrapFailed() == false;
    }

    private static PasswordPekEncryption pekEncryption(String node) {
        return internalCluster().getInstance(PasswordPekEncryption.class, node);
    }

    private static void reload(String node, Settings settings) {
        findPlugin(node, EncryptionPlugin.class).reload(settings);
    }
}
