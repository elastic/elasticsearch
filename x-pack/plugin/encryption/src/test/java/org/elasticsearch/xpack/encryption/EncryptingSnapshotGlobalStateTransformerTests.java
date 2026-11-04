/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class EncryptingSnapshotGlobalStateTransformerTests extends ESTestCase {

    private static final ProjectId PROJECT_ID = ProjectId.DEFAULT;
    private static final char[] GOOD_PASSWORD = "correcthorsebatterystaple".toCharArray();

    @After
    public void clearRegistry() {
        EncryptedDataHandlerRegistry.INSTANCE.set(null);
    }

    /** Minimal {@link Metadata.ProjectCustom} carrying one {@link EncryptedData}; context includes SNAPSHOT. */
    static final class TestBlob extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        static final String TYPE = "test_blob_for_transformer";
        final EncryptedData blob;

        TestBlob(EncryptedData blob) {
            this.blob = blob;
        }

        TestBlob(StreamInput in) throws IOException {
            this.blob = new EncryptedData(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            blob.writeTo(out);
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
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("blob", blob));
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }
    }

    static class TestBlobHandler implements EncryptedDataHandler<TestBlob> {
        @Override
        public String customName() {
            return TestBlob.TYPE;
        }

        @Override
        public TestBlob reEncrypt(TestBlob current, UnaryOperator<EncryptedData> rewrapper) {
            if (current == null) {
                return null;
            }
            EncryptedData rewrapped = rewrapper.apply(current.blob);
            if (rewrapped == null) {
                return null;
            }
            return rewrapped == current.blob ? current : new TestBlob(rewrapped);
        }
    }

    private static AesGcmEncryptionService buildService() {
        byte[] keyBytes = new byte[32];
        new SecureRandom().nextBytes(keyBytes);
        SecretKey key = new SecretKeySpec(keyBytes, "AES");
        AesGcmEncryptionService.KeyProvider kp = new AesGcmEncryptionService.KeyProvider() {
            @Override
            public AesGcmEncryptionService.ActiveKey getActiveKey() {
                return new AesGcmEncryptionService.ActiveKey("k1", key);
            }

            @Override
            public SecretKey getKey(String keyId) {
                return "k1".equals(keyId) ? key : null;
            }
        };
        return new AesGcmEncryptionService(kp, () -> EncryptionServiceState.READY, () -> true);
    }

    private static SnapshotEncryptedData password() {
        return new SnapshotEncryptedData(
            SnapshotEncryptedData.TYPE_PASSWORD,
            new SecureString(Arrays.copyOf(GOOD_PASSWORD, GOOD_PASSWORD.length)),
            null
        );
    }

    /** Sets up registry + service + metadata with one SNAPSHOT-context custom. */
    private record Setup(Metadata metadata, AesGcmEncryptionService svc, EncryptedData originalBlob) {}

    private Setup buildSetup() {
        AesGcmEncryptionService svc = buildService();
        byte[] plaintext = "secret-credential".getBytes(StandardCharsets.UTF_8);
        EncryptedData blob = svc.encrypt(plaintext);

        EncryptedDataHandlerRegistry.setInstance(new EncryptedDataHandlerRegistry(List.of(new TestBlobHandler())));
        EncryptionServiceRegistry.setEncryptionService(svc);

        TestBlob custom = new TestBlob(blob);
        ProjectMetadata project = ProjectMetadata.builder(PROJECT_ID).putCustom(TestBlob.TYPE, custom).build();
        Metadata metadata = Metadata.builder().put(project).build();
        return new Setup(metadata, svc, blob);
    }

    public void testWithPasswordRewrapsUnderSnapshotKey() {
        Setup s = buildSetup();
        EncryptingSnapshotGlobalStateTransformer t = new EncryptingSnapshotGlobalStateTransformer();
        assertTrue(t.containsEncryptedData(PROJECT_ID, s.metadata()));

        Metadata result = t.transformForSnapshot(PROJECT_ID, s.metadata(), password());

        assertThat(result, not(sameInstance(s.metadata())));
        TestBlob rewrapped = (TestBlob) result.getProject(PROJECT_ID).custom(TestBlob.TYPE);
        assertNotNull(rewrapped);
        assertThat(rewrapped.blob.keyId(), equalTo(EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID));
        assertThat(PasswordBasedEncryption.unwrap(rewrapped.blob, GOOD_PASSWORD.clone()), equalTo(s.svc().decrypt(s.originalBlob())));
    }

    public void testWithoutPasswordDropsEncryptedCustom() {
        Setup s = buildSetup();
        EncryptingSnapshotGlobalStateTransformer t = new EncryptingSnapshotGlobalStateTransformer();

        Metadata result = t.transformForSnapshot(PROJECT_ID, s.metadata(), null);

        assertThat(result, not(sameInstance(s.metadata())));
        assertNull(result.getProject(PROJECT_ID).custom(TestBlob.TYPE));
    }

    public void testUnsupportedTypeRejected() {
        Setup s = buildSetup();
        EncryptingSnapshotGlobalStateTransformer t = new EncryptingSnapshotGlobalStateTransformer();
        SnapshotEncryptedData secureSetting = new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_SECURE_SETTING, null, "id");

        expectThrows(IllegalStateException.class, () -> t.transformForSnapshot(PROJECT_ID, s.metadata(), secureSetting));
    }

    public void testCustomNotIncludedInSnapshotsIsLeftAlone() {
        Setup s = buildSetup();
        EncryptedDataHandlerRegistry.setInstance(new EncryptedDataHandlerRegistry(List.of(new EncryptedDataHandler<TestBlob>() {
            @Override
            public String customName() {
                return TestBlob.TYPE;
            }

            @Override
            public boolean includedInSnapshots() {
                return false;
            }

            @Override
            public TestBlob reEncrypt(TestBlob current, UnaryOperator<EncryptedData> rewrapper) {
                throw new AssertionError("must not be called");
            }
        })));
        EncryptingSnapshotGlobalStateTransformer t = new EncryptingSnapshotGlobalStateTransformer();

        assertFalse(t.containsEncryptedData(PROJECT_ID, s.metadata()));
        assertThat(t.transformForSnapshot(PROJECT_ID, s.metadata(), randomBoolean() ? null : password()), sameInstance(s.metadata()));
    }

    public void testNoHandlersIsNoOp() {
        Setup s = buildSetup();
        EncryptedDataHandlerRegistry.setInstance(randomBoolean() ? null : new EncryptedDataHandlerRegistry(List.of()));
        EncryptingSnapshotGlobalStateTransformer t = new EncryptingSnapshotGlobalStateTransformer();

        assertThat(t.transformForSnapshot(PROJECT_ID, s.metadata(), password()), sameInstance(s.metadata()));
        assertThat(t.transformForSnapshot(PROJECT_ID, s.metadata(), null), sameInstance(s.metadata()));
        assertFalse(t.containsEncryptedData(PROJECT_ID, s.metadata()));
    }

    public void testKeyRetiredWhileWaitingToFinalize() {
        assertKeyUnavailableAtFinalization(true, "project encryption key [k2] was rotated and retired while the snapshot was waiting");
    }

    public void testKeyUnknownAtFinalization() {
        assertKeyUnavailableAtFinalization(false, "cannot re-encrypt data under project encryption key [k2] with the snapshot password");
    }

    private void assertKeyUnavailableAtFinalization(boolean keyInCapturedMetadata, String expectedReason) {
        buildSetup();
        ProjectMetadata.Builder project = ProjectMetadata.builder(PROJECT_ID)
            .putCustom(TestBlob.TYPE, new TestBlob(new EncryptedData("k2", randomByteArrayOfLength(32))));
        if (keyInCapturedMetadata) {
            project.putCustom(
                ProjectEncryptionKeyMetadata.TYPE,
                new ProjectEncryptionKeyMetadata(
                    Map.of("k2", new ProjectEncryptionKeyMetadata.KeyEntry(randomByteArrayOfLength(32), 0L)),
                    "k2",
                    "password-id",
                    Map.of(),
                    TestPekEncryption.NO_OP
                )
            );
        }
        Metadata metadata = Metadata.builder().put(project.build()).build();

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new EncryptingSnapshotGlobalStateTransformer().transformForSnapshot(PROJECT_ID, metadata, password())
        );
        assertThat(e.getMessage(), containsString(expectedReason));
        assertThat(e.getMessage(), containsString("the snapshot was not written to the repository and can be retried"));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(e.getCause(), instanceOf(EncryptionKeyNotYetAvailableException.class));
    }

    public void testCustomWithoutDataIsNotEncryptedData() {
        Setup s = buildSetup();
        EncryptedDataHandlerRegistry.setInstance(new EncryptedDataHandlerRegistry(List.of(new TestBlobHandler() {
            @Override
            public boolean hasData(TestBlob current) {
                return false;
            }
        })));
        EncryptingSnapshotGlobalStateTransformer t = new EncryptingSnapshotGlobalStateTransformer();

        assertFalse(t.containsEncryptedData(PROJECT_ID, s.metadata()));
        assertThat(t.transformForSnapshot(PROJECT_ID, s.metadata(), randomBoolean() ? null : password()), sameInstance(s.metadata()));
    }
}
