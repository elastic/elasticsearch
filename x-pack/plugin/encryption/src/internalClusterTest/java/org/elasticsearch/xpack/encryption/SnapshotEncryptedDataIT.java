/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsInProgressEncryptedData;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that PEK-encrypted project data supplied with a snapshot password ends up in the snapshot re-wrapped under that password,
 * across master failover, concurrent snapshots and clones, that it is excluded without a password, and that the password never leaves
 * the cluster in a cluster state response.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotEncryptedDataIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String SNAPSHOT_PASSWORD = "correct-horse-battery-staple";

    @BeforeClass
    public static void checkFeatureFlagEnabled() {
        assumeTrue("snapshot_encrypted_data feature flag must be enabled", SnapshotEncryptedData.FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v1");
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1", "encryption-test-password");
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).setSecureSettings(secure).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        plugins.add(TestBlobPlugin.class);
        return plugins;
    }

    public void testEncryptedDataSurvivesMasterFailover() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final byte[] secret = randomByteArrayOfLength(32);
        seedEncryptedBlob(secret);
        createRepository(REPO, "mock");
        createIndexWithContent("index");

        blockDataNode(REPO, dataNode);
        final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, "snap").includeGlobalState(true)
            .waitForCompletion(false)
            .encryptedData(
                new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, new SecureString(SNAPSHOT_PASSWORD.toCharArray()), "pw-1")
            );
        safeGet(client(dataNode).execute(TransportCreateSnapshotAction.TYPE, request));
        waitForBlock(dataNode, REPO);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);
        unblockNode(REPO, dataNode);
        awaitNoMoreRunningOperations();

        final SnapshotInfo snapshotInfo = getSnapshot(REPO, "snap");
        final ClusterState masterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertNull(SnapshotsInProgressEncryptedData.get(masterState).get(snapshotInfo.snapshot()));
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.encryptedDataType(), equalTo(SnapshotEncryptedData.TYPE_PASSWORD));
        assertThat(snapshotInfo.encryptedDataPasswordId(), equalTo("pw-1"));

        final TestBlob stored = blobInSnapshot(snapshotInfo);
        assertThat(stored, notNullValue());
        assertThat(stored.blob.keyId(), equalTo(EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID));
        assertArrayEquals(secret, PasswordBasedEncryption.unwrap(stored.blob, SNAPSHOT_PASSWORD.toCharArray()));
    }

    public void testEncryptedDataExcludedWithoutPassword() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        seedEncryptedBlob(randomByteArrayOfLength(32));
        createRepository(REPO, "fs");
        createIndexWithContent("index");

        final SnapshotInfo snapshotInfo = createFullSnapshot(REPO, "snap");

        assertFalse(snapshotInfo.hasEncryptedData());
        assertThat(snapshotInfo.encryptedDataType(), equalTo(SnapshotEncryptedData.TYPE_NONE));
        assertNull(blobInSnapshot(snapshotInfo));

        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, REPO, "snap", "clone").setIndices("index"));
        assertThat(getSnapshot(REPO, "clone").encryptedDataType(), equalTo(SnapshotEncryptedData.TYPE_NONE));
    }

    public void testEncryptedDataRequiredRejectsSnapshotWithoutPassword() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        seedEncryptedBlob(randomByteArrayOfLength(32));
        createRepository(REPO, "fs");
        createIndexWithContent("index");
        updateClusterSettings(Settings.builder().put(SnapshotsService.ENCRYPTED_DATA_REQUIRED_SETTING.getKey(), true));

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, REPO, "snap").setWaitForCompletion(true)
        );
        assertThat(e.getMessage(), containsString("[snapshot.encrypted_data.required] is true"));

        final SnapshotInfo withoutGlobalState = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, REPO, "snap-no-global-state")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(withoutGlobalState.state(), equalTo(SnapshotState.SUCCESS));
    }

    public void testPasswordNotInClusterStateResponse() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        seedEncryptedBlob(randomByteArrayOfLength(32));
        createRepository(REPO, "mock");
        createIndexWithContent("index");

        blockDataNode(REPO, dataNode);
        final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, "snap").includeGlobalState(true)
            .waitForCompletion(false)
            .encryptedData(
                new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, new SecureString(SNAPSHOT_PASSWORD.toCharArray()), null)
            );
        safeGet(client(dataNode).execute(TransportCreateSnapshotAction.TYPE, request));
        waitForBlock(dataNode, REPO);

        // sent from a non-master node so that the response is read back from the transport layer
        final ClusterStateResponse response = safeGet(
            client(dataNode).execute(ClusterStateAction.INSTANCE, new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
        );
        final SnapshotsInProgress.Entry entry = SnapshotsInProgress.get(response.getState()).asStream().findFirst().orElseThrow();
        assertThat(entry.snapshot().getSnapshotId().getName(), equalTo("snap"));
        assertNull(response.getState().custom(SnapshotsInProgressEncryptedData.TYPE));
        final ClusterState masterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertNotNull(SnapshotsInProgressEncryptedData.get(masterState).get(entry.snapshot()));

        unblockNode(REPO, dataNode);
        awaitNoMoreRunningOperations();
    }

    public void testConcurrentSnapshotsWithDifferentPasswords() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final byte[] secret = randomByteArrayOfLength(32);
        seedEncryptedBlob(secret);
        createRepository(REPO, "mock");
        createIndexWithContent("index");

        final Map<String, String> passwords = Map.of("snap-1", SNAPSHOT_PASSWORD + "-1", "snap-2", SNAPSHOT_PASSWORD + "-2");
        blockDataNode(REPO, dataNode);
        for (Map.Entry<String, String> snapshot : passwords.entrySet()) {
            final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, snapshot.getKey())
                .includeGlobalState(true)
                .waitForCompletion(false)
                .encryptedData(
                    new SnapshotEncryptedData(
                        SnapshotEncryptedData.TYPE_PASSWORD,
                        new SecureString(snapshot.getValue().toCharArray()),
                        snapshot.getKey()
                    )
                );
            safeGet(client(dataNode).execute(TransportCreateSnapshotAction.TYPE, request));
        }
        waitForBlock(dataNode, REPO);
        final ClusterState masterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(SnapshotsInProgress.get(masterState).count(), equalTo(2));
        SnapshotsInProgress.get(masterState).asStream().forEach(entry -> {
            final SnapshotEncryptedData encryptedData = SnapshotsInProgressEncryptedData.get(masterState).get(entry.snapshot());
            assertThat(encryptedData.password().toString(), equalTo(passwords.get(entry.snapshot().getSnapshotId().getName())));
        });
        unblockNode(REPO, dataNode);
        awaitNoMoreRunningOperations();

        for (Map.Entry<String, String> snapshot : passwords.entrySet()) {
            final SnapshotInfo snapshotInfo = getSnapshot(REPO, snapshot.getKey());
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfo.encryptedDataPasswordId(), equalTo(snapshot.getKey()));
            final TestBlob stored = blobInSnapshot(snapshotInfo);
            assertArrayEquals(secret, PasswordBasedEncryption.unwrap(stored.blob, snapshot.getValue().toCharArray()));
            final String otherPassword = passwords.get(snapshot.getKey().equals("snap-1") ? "snap-2" : "snap-1");
            expectThrows(ElasticsearchException.class, () -> PasswordBasedEncryption.unwrap(stored.blob, otherPassword.toCharArray()));
        }
    }

    public void testCloneKeepsEncryptedData() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final byte[] secret = randomByteArrayOfLength(32);
        seedEncryptedBlob(secret);
        createRepository(REPO, "fs");
        createIndexWithContent("index");

        final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, "snap").includeGlobalState(true)
            .waitForCompletion(true)
            .encryptedData(
                new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, new SecureString(SNAPSHOT_PASSWORD.toCharArray()), "pw-1")
            );
        safeGet(client().execute(TransportCreateSnapshotAction.TYPE, request));
        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, REPO, "snap", "clone").setIndices("index"));

        final SnapshotInfo clone = getSnapshot(REPO, "clone");
        assertThat(clone.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(clone.encryptedDataType(), equalTo(SnapshotEncryptedData.TYPE_PASSWORD));
        assertThat(clone.encryptedDataPasswordId(), equalTo("pw-1"));
        final TestBlob stored = blobInSnapshot(clone);
        assertThat(stored, notNullValue());
        assertArrayEquals(secret, PasswordBasedEncryption.unwrap(stored.blob, SNAPSHOT_PASSWORD.toCharArray()));
    }

    /**
     * A value that can't be decrypted for re-wrapping, here because its key is unknown, fails the snapshot at finalization without
     * writing it to the repository, drops its password from cluster state, and doesn't block later snapshots.
     */
    public void testFinalizationFailureFailsSnapshotCleanly() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        awaitProjectEncryptionKey();
        seedBlob(new EncryptedData("unknown-key", randomByteArrayOfLength(32)));
        createRepository(REPO, "fs");
        createIndexWithContent("index");

        final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, "snap").includeGlobalState(true)
            .waitForCompletion(true)
            .encryptedData(
                new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, new SecureString(SNAPSHOT_PASSWORD.toCharArray()), null)
            );
        final Exception e = safeAwaitFailure(
            CreateSnapshotResponse.class,
            l -> client(masterNode).execute(TransportCreateSnapshotAction.TYPE, request, l)
        );
        assertThat(e.getMessage(), containsString("cannot re-encrypt data under project encryption key [unknown-key]"));
        awaitNoMoreRunningOperations();

        assertThat(getRepositoryData(REPO).getSnapshotIds(), empty());
        final ClusterState masterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(SnapshotsInProgressEncryptedData.get(masterState), equalTo(SnapshotsInProgressEncryptedData.EMPTY));

        assertThat(createFullSnapshot(REPO, "snap-without-password").state(), equalTo(SnapshotState.SUCCESS));
    }

    private TestBlob blobInSnapshot(SnapshotInfo snapshotInfo) {
        final Repository repository = getRepositoryOnMaster(REPO);
        return repository.getSnapshotGlobalMetadata(snapshotInfo.snapshotId(), false).getProject(ProjectId.DEFAULT).custom(TestBlob.TYPE);
    }

    /**
     * Waits for the initial project encryption key, then stores {@code secret} encrypted under it in a SNAPSHOT-context custom.
     */
    private void seedEncryptedBlob(byte[] secret) throws Exception {
        awaitProjectEncryptionKey();
        seedBlob(internalCluster().getCurrentMasterNodeInstance(EncryptionService.class).encrypt(secret));
    }

    private void awaitProjectEncryptionKey() throws Exception {
        assertBusy(() -> {
            final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
            assertThat(clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE), notNullValue());
        });
    }

    private void seedBlob(EncryptedData encrypted) {
        safeAwait(
            listener -> internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .submitUnbatchedStateUpdateTask("seed-test-blob", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState.copyAndUpdateProject(
                            ProjectId.DEFAULT,
                            b -> b.putCustom(TestBlob.TYPE, new TestBlob(encrypted))
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                        listener.onResponse(null);
                    }
                })
        );
    }

    /** Registers {@link TestBlob}; its presence on a node also enables {@link TestBlobHandlerProvider}. */
    public static class TestBlobPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TestBlob.TYPE, TestBlob::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, TestBlob.TYPE, TestBlob::readDiffFrom)
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(TestBlob.TYPE), TestBlob::fromXContent)
            );
        }
    }

    /**
     * Loaded via {@code META-INF/services}. The constructor ties it to {@link TestBlobPlugin} so that other tests in this source set,
     * which don't install that plugin, don't get its handler.
     */
    public static final class TestBlobHandlerProvider implements EncryptedDataHandlerProvider {
        public TestBlobHandlerProvider(TestBlobPlugin plugin) {}

        @Override
        public Collection<EncryptedDataHandler<?>> getHandlers() {
            return List.of(new TestBlobHandler());
        }
    }

    private static final class TestBlobHandler implements EncryptedDataHandler<TestBlob> {
        @Override
        public String customName() {
            return TestBlob.TYPE;
        }

        @Override
        public TestBlob reEncrypt(TestBlob current, UnaryOperator<EncryptedData> rewrapper) {
            if (current == null) {
                return null;
            }
            final EncryptedData rewrapped = rewrapper.apply(current.blob);
            if (rewrapped == null) {
                return null;
            }
            return rewrapped == current.blob ? current : new TestBlob(rewrapped);
        }
    }

    public static final class TestBlob extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        static final String TYPE = "snapshot_encrypted_data_it_blob";

        private static final ConstructingObjectParser<TestBlob, Void> PARSER = new ConstructingObjectParser<>(
            TYPE,
            args -> new TestBlob((EncryptedData) args[0])
        );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> EncryptedData.fromXContent(p), new ParseField("blob"));
        }

        final EncryptedData blob;

        TestBlob(EncryptedData blob) {
            this.blob = blob;
        }

        public TestBlob(StreamInput in) throws IOException {
            this.blob = new EncryptedData(in);
        }

        static TestBlob fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            blob.writeTo(out);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("blob", blob));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestBlob other && other.blob.equals(blob);
        }

        @Override
        public int hashCode() {
            return blob.hashCode();
        }
    }
}
