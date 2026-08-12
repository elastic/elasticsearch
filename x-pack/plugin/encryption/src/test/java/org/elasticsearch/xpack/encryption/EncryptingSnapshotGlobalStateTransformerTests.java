/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the in-place re-wrapping in {@link EncryptingSnapshotGlobalStateTransformer}: PEK-wrapped values are
 * re-encrypted under the snapshot password when one is given, excluded when not, and recovered (or cleared with a
 * warning) at restore time.
 */
public class EncryptingSnapshotGlobalStateTransformerTests extends ESTestCase {

    private static final String PEK_KEY_ID = "pek-key-1";
    private static final SecureString PASSWORD = new SecureString("a-perfectly-valid-password".toCharArray());
    private static final byte[] PLAINTEXT = "policy-secret".getBytes(StandardCharsets.UTF_8);

    private final EncryptingSnapshotGlobalStateTransformer transformer = new EncryptingSnapshotGlobalStateTransformer();
    private AesGcmEncryptionService encryptionService;

    @Before
    public void setUpRegistries() {
        SecretKey key = new SecretKeySpec(randomByteArrayOfLength(32), "AES");
        // real crypto with a fixed single key; the KeyProvider stub replaces the cluster-state-backed provider
        AesGcmEncryptionService.KeyProvider keyProvider = new AesGcmEncryptionService.KeyProvider() {
            @Override
            public AesGcmEncryptionService.ActiveKey getActiveKey() {
                return new AesGcmEncryptionService.ActiveKey(PEK_KEY_ID, key);
            }

            @Override
            public SecretKey getKey(String keyId) {
                return PEK_KEY_ID.equals(keyId) ? key : null;
            }
        };
        encryptionService = new AesGcmEncryptionService(keyProvider, () -> EncryptionServiceState.READY, () -> true);
        EncryptionServiceRegistry.setEncryptionService(encryptionService);
        EncryptedDataHandlerRegistry.setInstance(new EncryptedDataHandlerRegistry(List.of(new TestSecretHandler())));
    }

    @After
    public void resetRegistries() {
        EncryptionServiceRegistry.reset();
        EncryptedDataHandlerRegistry.reset();
        EncryptingSnapshotGlobalStateTransformer.setEncryptedDataRequired(false);
    }

    public void testSnapshotWithPasswordRewrapsUnderSnapshotPasswordKey() {
        Metadata metadata = metadataWith(new TestSecretCustom(encryptionService.encrypt(PLAINTEXT), Metadata.ALL_CONTEXTS));

        Metadata transformed = transformer.transformForSnapshot(ProjectId.DEFAULT, metadata, createRequest(PASSWORD));

        assertNotSame(metadata, transformed);
        EncryptedData rewrapped = customOf(transformed).secret();
        assertThat(rewrapped.keyId(), equalTo(EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID));
        assertArrayEquals(PLAINTEXT, PasswordBasedEncryption.unwrap(rewrapped, PASSWORD.getChars()));
    }

    public void testSnapshotWithoutPasswordExcludesEncryptedValues() {
        Metadata metadata = metadataWith(new TestSecretCustom(encryptionService.encrypt(PLAINTEXT), Metadata.ALL_CONTEXTS));

        Metadata[] transformed = new Metadata[1];
        MockLog.assertThatLogger(
            () -> transformed[0] = transformer.transformForSnapshot(ProjectId.DEFAULT, metadata, createRequest(null)),
            EncryptingSnapshotGlobalStateTransformer.class,
            new MockLog.SeenEventExpectation(
                "exclusion warning",
                EncryptingSnapshotGlobalStateTransformer.class.getCanonicalName(),
                Level.WARN,
                "Encrypted data exists but no password was provided; it was excluded from the snapshot. "
                    + "Set encrypted_data_password to include it."
            )
        );
        assertWarnings(
            "Encrypted data exists but no password was provided; it was excluded from the snapshot. "
                + "Set encrypted_data_password to include it."
        );

        assertNotSame(metadata, transformed[0]);
        assertThat(customOf(transformed[0]).secret(), nullValue());
    }

    public void testStrictModeFailsSnapshotWithEncryptedDataButNoPassword() {
        EncryptingSnapshotGlobalStateTransformer.setEncryptedDataRequired(true);
        Metadata metadata = metadataWith(new TestSecretCustom(encryptionService.encrypt(PLAINTEXT), Metadata.ALL_CONTEXTS));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> transformer.transformForSnapshot(ProjectId.DEFAULT, metadata, createRequest(null))
        );
        assertThat(e.getMessage(), containsString("snapshot.encrypted_data.required"));
        assertThat(e.getMessage(), containsString("encrypted_data_password"));
    }

    public void testStrictModeIsInertWithPasswordOrWithoutEncryptedData() {
        EncryptingSnapshotGlobalStateTransformer.setEncryptedDataRequired(true);

        Metadata withSecret = metadataWith(new TestSecretCustom(encryptionService.encrypt(PLAINTEXT), Metadata.ALL_CONTEXTS));
        EncryptedData rewrapped = customOf(transformer.transformForSnapshot(ProjectId.DEFAULT, withSecret, createRequest(PASSWORD)))
            .secret();
        assertThat(rewrapped.keyId(), equalTo(EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID));

        Metadata withoutSecret = metadataWith(new TestSecretCustom(null, Metadata.ALL_CONTEXTS));
        assertThat(transformer.transformForSnapshot(ProjectId.DEFAULT, withoutSecret, createRequest(null)), sameInstance(withoutSecret));
    }

    public void testSnapshotWithoutPasswordAndWithoutEncryptedDataIsUntouched() {
        Metadata metadata = metadataWith(new TestSecretCustom(null, Metadata.ALL_CONTEXTS));

        assertThat(transformer.transformForSnapshot(ProjectId.DEFAULT, metadata, createRequest(null)), sameInstance(metadata));
        assertThat(transformer.transformForSnapshot(ProjectId.DEFAULT, metadata, null), sameInstance(metadata));
    }

    public void testSnapshotSkipsCustomsWithoutSnapshotContext() {
        Metadata metadata = metadataWith(
            new TestSecretCustom(encryptionService.encrypt(PLAINTEXT), EnumSet.of(Metadata.XContentContext.GATEWAY))
        );

        assertThat(transformer.transformForSnapshot(ProjectId.DEFAULT, metadata, createRequest(PASSWORD)), sameInstance(metadata));
    }

    public void testRestoreWithPasswordReencryptsUnderPek() {
        EncryptedData passwordWrapped = PasswordBasedEncryption.wrap(
            PLAINTEXT,
            EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID,
            PASSWORD.getChars()
        );
        Metadata restored = metadataWith(new TestSecretCustom(passwordWrapped, Metadata.ALL_CONTEXTS));

        Metadata transformed = transformer.transformForRestore(ProjectId.DEFAULT, restored, restoreRequest(PASSWORD));

        assertNotSame(restored, transformed);
        EncryptedData rewrapped = customOf(transformed).secret();
        assertThat(rewrapped, notNullValue());
        assertThat(rewrapped.keyId(), equalTo(PEK_KEY_ID));
        assertArrayEquals(PLAINTEXT, encryptionService.decrypt(rewrapped));
    }

    public void testRestoreWithoutPasswordClearsPasswordWrappedValues() {
        EncryptedData passwordWrapped = PasswordBasedEncryption.wrap(
            PLAINTEXT,
            EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID,
            PASSWORD.getChars()
        );
        Metadata restored = metadataWith(new TestSecretCustom(passwordWrapped, Metadata.ALL_CONTEXTS));

        Metadata[] transformed = new Metadata[1];
        MockLog.assertThatLogger(
            () -> transformed[0] = transformer.transformForRestore(ProjectId.DEFAULT, restored, restoreRequest(null)),
            EncryptingSnapshotGlobalStateTransformer.class,
            new MockLog.SeenEventExpectation(
                "restore exclusion warning",
                EncryptingSnapshotGlobalStateTransformer.class.getCanonicalName(),
                Level.WARN,
                "Encrypted data exists but no password was provided; it was excluded when restoring the snapshot. "
                    + "Set encrypted_data_password to include it."
            )
        );

        assertThat(customOf(transformed[0]).secret(), nullValue());
    }

    public void testRestoreClearsValuesUnderUnknownKeys() {
        // snapshots only ever contain password-wrapped values; a foreign PEK id must be cleared, not restored verbatim
        Metadata restored = metadataWith(
            new TestSecretCustom(new EncryptedData("some-other-clusters-pek", randomByteArrayOfLength(32)), Metadata.ALL_CONTEXTS)
        );

        Metadata transformed = transformer.transformForRestore(ProjectId.DEFAULT, restored, restoreRequest(PASSWORD));

        assertThat(customOf(transformed).secret(), nullValue());
    }

    public void testRestoreWithPasswordButNoEncryptedDataWarns() {
        Metadata restored = metadataWith(new TestSecretCustom(null, Metadata.ALL_CONTEXTS));

        Metadata[] transformed = new Metadata[1];
        MockLog.assertThatLogger(
            () -> transformed[0] = transformer.transformForRestore(ProjectId.DEFAULT, restored, restoreRequest(PASSWORD)),
            EncryptingSnapshotGlobalStateTransformer.class,
            new MockLog.SeenEventExpectation(
                "unused password warning",
                EncryptingSnapshotGlobalStateTransformer.class.getCanonicalName(),
                Level.WARN,
                "an encrypted_data_password was provided but the restored global state contains no encrypted data"
            )
        );
        assertThat(transformed[0], sameInstance(restored));
    }

    public void testRestoreWithWrongPasswordThrows() {
        EncryptedData passwordWrapped = PasswordBasedEncryption.wrap(
            PLAINTEXT,
            EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID,
            PASSWORD.getChars()
        );
        Metadata restored = metadataWith(new TestSecretCustom(passwordWrapped, Metadata.ALL_CONTEXTS));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> transformer.transformForRestore(
                ProjectId.DEFAULT,
                restored,
                restoreRequest(new SecureString("a-different-but-valid-pass".toCharArray()))
            )
        );
        assertThat(e.getMessage(), containsString("unwrap failed"));
    }

    private static CreateSnapshotRequest createRequest(@Nullable SecureString password) {
        return new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap").encryptedDataPassword(password);
    }

    private static RestoreSnapshotRequest restoreRequest(@Nullable SecureString password) {
        return new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap").encryptedDataPassword(password);
    }

    private static Metadata metadataWith(TestSecretCustom custom) {
        return Metadata.builder().put(ProjectMetadata.builder(ProjectId.DEFAULT).putCustom(TestSecretCustom.TYPE, custom)).build();
    }

    private static TestSecretCustom customOf(Metadata metadata) {
        return metadata.getProject(ProjectId.DEFAULT).custom(TestSecretCustom.TYPE);
    }

    /** Applies the re-keying function to the single secret; clears it on a {@code null} result. */
    private static final class TestSecretHandler implements EncryptedDataHandler<TestSecretCustom> {
        @Override
        public String customName() {
            return TestSecretCustom.TYPE;
        }

        @Override
        public TestSecretCustom reEncrypt(TestSecretCustom current, UnaryOperator<EncryptedData> reEncrypt) {
            if (current.secret == null) {
                return current;
            }
            EncryptedData rewrapped = reEncrypt.apply(current.secret);
            return rewrapped == current.secret ? current : new TestSecretCustom(rewrapped, current.context);
        }
    }

    /** Minimal project custom holding one optional secret, with a configurable xcontent context. */
    private static final class TestSecretCustom extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        static final String TYPE = "transformer_test_secret";

        @Nullable
        private final EncryptedData secret;
        private final EnumSet<Metadata.XContentContext> context;

        TestSecretCustom(@Nullable EncryptedData secret, EnumSet<Metadata.XContentContext> context) {
            this.secret = secret;
            this.context = context;
        }

        @Nullable
        EncryptedData secret() {
            return secret;
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
        public void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) throws IOException {
            out.writeOptionalWriteable(secret);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return context;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("secret", secret));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestSecretCustom other && Objects.equals(secret, other.secret) && context.equals(other.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(secret, context);
        }
    }
}
