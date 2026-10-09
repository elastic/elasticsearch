/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.PekEncryption;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ProjectEncryptionKeyMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.ProjectCustom> {

    private static final String PASSWORD_ID = "v1";

    static final PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;

    private static byte[] randomPlaintextBytes() {
        return randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
    }

    private record RandomEntries(Map<String, KeyEntry> entries, String activeKeyId) {}

    private static RandomEntries randomEntriesWithLastActive(int count) {
        Map<String, KeyEntry> entries = new HashMap<>();
        long ts = randomLongBetween(0L, Long.MAX_VALUE - count);
        String lastId = null;
        for (int i = 0; i < count; i++) {
            lastId = ProjectEncryptionKeyMetadata.generateKeyId();
            entries.put(lastId, new KeyEntry(randomPlaintextBytes(), ts++));
        }
        return new RandomEntries(entries, lastId);
    }

    private static ProjectEncryptionKeyMetadata randomPekMetadata() {
        RandomEntries r = randomEntriesWithLastActive(randomIntBetween(1, 5));
        return new ProjectEncryptionKeyMetadata(r.entries(), r.activeKeyId(), PASSWORD_ID, Map.of(), NO_OP_ENCRYPTION);
    }

    @Override
    protected Metadata.ProjectCustom createTestInstance() {
        return randomPekMetadata();
    }

    @Override
    protected Metadata.ProjectCustom mutateInstance(Metadata.ProjectCustom instance) {
        ProjectEncryptionKeyMetadata pek = (ProjectEncryptionKeyMetadata) instance;
        Map<String, KeyEntry> entries = new HashMap<>(pek.getKeys());
        String activeId = pek.getActiveKeyId();
        long activeTs = entries.get(activeId).generatedAt();
        boolean addNew = entries.size() == 1 || randomBoolean();
        if (addNew && activeTs < Long.MAX_VALUE) {
            String newId = ProjectEncryptionKeyMetadata.generateKeyId();
            long newTs = randomLongBetween(activeTs + 1, Long.MAX_VALUE);
            entries.put(newId, new KeyEntry(randomPlaintextBytes(), newTs));
            return new ProjectEncryptionKeyMetadata(entries, newId, pek.getPasswordId(), Map.of(), NO_OP_ENCRYPTION);
        }
        String id = randomValueOtherThan(activeId, () -> randomFrom(entries.keySet()));
        KeyEntry old = entries.get(id);
        long newTs = randomValueOtherThan(old.generatedAt(), () -> randomLongBetween(0L, activeTs - 1));
        entries.put(id, new KeyEntry(old.bytes(), newTs));
        return new ProjectEncryptionKeyMetadata(entries, activeId, pek.getPasswordId(), Map.of(), NO_OP_ENCRYPTION);
    }

    @Override
    protected Metadata.ProjectCustom doParseInstance(XContentParser parser) throws IOException {
        return ProjectEncryptionKeyMetadata.fromXContent(parser, NO_OP_ENCRYPTION, new ProjectEncryptionKeyMetadata.DegradedBlobHolder());
    }

    @Override
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return in -> new ProjectEncryptionKeyMetadata(in, NO_OP_ENCRYPTION, new ProjectEncryptionKeyMetadata.DegradedBlobHolder());
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.ProjectCustom>> diffReader() {
        return ProjectEncryptionKeyMetadata::readDiffFrom;
    }

    @Override
    protected Metadata.ProjectCustom makeTestChanges(Metadata.ProjectCustom testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    ProjectEncryptionKeyMetadata.TYPE,
                    in -> new ProjectEncryptionKeyMetadata(in, NO_OP_ENCRYPTION, new ProjectEncryptionKeyMetadata.DegradedBlobHolder())
                )
            )
        );
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
    }

    public void testContextIsGatewayAndApi() {
        ProjectEncryptionKeyMetadata metadata = randomPekMetadata();
        assertEquals(EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.API), metadata.context());
    }

    public void testApiContextRedactsBytesButKeepsOperationalFields() throws IOException {
        ProjectEncryptionKeyMetadata metadata = randomPekMetadata();
        String apiJson = chunkedToXContent(metadata, new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, "API")));
        String gatewayJson = chunkedToXContent(metadata, new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, "GATEWAY")));

        assertThat(apiJson, containsString("\"active_key_id\":\"" + metadata.getActiveKeyId() + "\""));
        assertThat(apiJson, containsString("\"password_id\":\"" + metadata.getPasswordId() + "\""));
        assertThat(apiJson, containsString("\"generated_at\""));
        assertThat("API context must not emit a 'bytes' field", apiJson, not(containsString("\"bytes\"")));
        assertThat("GATEWAY context must emit 'bytes' for disk round-trip", gatewayJson, containsString("\"bytes\""));
        for (KeyEntry entry : metadata.getKeys().values()) {
            String base64 = java.util.Base64.getEncoder().encodeToString(entry.bytes());
            assertThat("API context must not leak plaintext key bytes (base64)", apiJson, not(containsString(base64)));
        }
    }

    private static String chunkedToXContent(ChunkedToXContent instance, ToXContent.Params params) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        instance.toXContentChunked(params).forEachRemaining(c -> {
            try {
                c.toXContent(builder, params);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        builder.endObject();
        return Strings.toString(builder);
    }

    public void testGetGeneratedAtForMissingKeyId() {
        ProjectEncryptionKeyMetadata metadata = randomPekMetadata();
        assertEquals(0L, metadata.getGeneratedAt("nonexistent"));
    }

    public void testToStringDoesNotLeakKey() {
        ProjectEncryptionKeyMetadata metadata = randomPekMetadata();
        String str = metadata.toString();
        for (KeyEntry entry : metadata.getKeys().values()) {
            assertFalse(str.contains(java.util.Base64.getEncoder().encodeToString(entry.bytes())));
        }
        assertTrue(str.contains("[keys secret]"));
        assertTrue(str.contains("passwordId=" + PASSWORD_ID));
    }

    public void testActiveKeyIdNotInEntries() {
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        expectThrows(
            IllegalArgumentException.class,
            () -> new ProjectEncryptionKeyMetadata(
                Map.of(keyId, new KeyEntry(randomPlaintextBytes(), 0L)),
                "nonexistent",
                PASSWORD_ID,
                Map.of(),
                NO_OP_ENCRYPTION
            )
        );
    }

    public void testEmptyEntriesMap() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ProjectEncryptionKeyMetadata(Map.of(), "any", PASSWORD_ID, Map.of(), NO_OP_ENCRYPTION)
        );
    }

    public void testNullPasswordIdRejected() {
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        expectThrows(
            NullPointerException.class,
            () -> new ProjectEncryptionKeyMetadata(
                Map.of(keyId, new KeyEntry(randomPlaintextBytes(), 0L)),
                keyId,
                null,
                Map.of(),
                NO_OP_ENCRYPTION
            )
        );
    }

    public void testFromXContentMissingActiveKeyId() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"password_id\":\"v1\",\"keys\":{\"abc\":{\"bytes\":\"AAAA\",\"generated_at\":0}}}"
            )
        ) {
            expectThrows(
                IllegalArgumentException.class,
                () -> ProjectEncryptionKeyMetadata.fromXContent(
                    parser,
                    NO_OP_ENCRYPTION,
                    new ProjectEncryptionKeyMetadata.DegradedBlobHolder()
                )
            );
        }
    }

    public void testFromXContentMissingKeys() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"active_key_id\":\"abc\",\"password_id\":\"v1\"}")) {
            expectThrows(
                IllegalArgumentException.class,
                () -> ProjectEncryptionKeyMetadata.fromXContent(
                    parser,
                    NO_OP_ENCRYPTION,
                    new ProjectEncryptionKeyMetadata.DegradedBlobHolder()
                )
            );
        }
    }

    public void testFromXContentMissingPasswordIdDefaultsToEmpty() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"active_key_id\":\"abc\",\"keys\":{\"abc\":{\"bytes\":\"AAAA\",\"generated_at\":0}}}"
            )
        ) {
            ProjectEncryptionKeyMetadata reconstructed = ProjectEncryptionKeyMetadata.fromXContent(
                parser,
                NO_OP_ENCRYPTION,
                new ProjectEncryptionKeyMetadata.DegradedBlobHolder()
            );
            assertEquals("", reconstructed.getPasswordId());
            assertEquals("abc", reconstructed.getActiveKeyId());
        }
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomPlaintextBytes(), generatedAt);
    }

    public void testFindRetireableKeyIdsExcludesActive() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L), "k2", entry(100L)),
            "k2",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(Long.MAX_VALUE));
    }

    public void testFindRetireableKeyIdsUsesDeactivationTimeNotGenerationTime() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(100L), "k2", entry(400L), "k3", entry(500L)),
            "k3",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(450L));
    }

    public void testFindRetireableKeyIdsReturnsEmptyWhenNoNonActiveKeys() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("only", entry(100L)),
            "only",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        assertTrue(metadata.findRetireableKeyIds(Long.MAX_VALUE).isEmpty());
    }

    public void testWrappedKeyCacheInvalidatedWhenActivePasswordChanges() throws IOException {
        AtomicInteger wrapCount = new AtomicInteger();
        AtomicReference<String> activeId = new AtomicReference<>("v1");
        PekEncryption tracking = new PekEncryption() {
            @Override
            public String activePasswordId() {
                return activeId.get();
            }

            @Override
            public WrappedKey wrap(byte[] plaintext) {
                wrapCount.incrementAndGet();
                return new WrappedKey(activeId.get(), plaintext.clone());
            }

            @Override
            public byte[] unwrap(byte[] wrapped, String passwordId) {
                return wrapped.clone();
            }
        };

        ToXContent.Params gatewayParams = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", new KeyEntry(randomPlaintextBytes(), 0L)),
            "k1",
            "v1",
            Map.of(),
            tracking
        );

        // First GATEWAY serialization triggers one wrap per key.
        chunkedToXContent(metadata, gatewayParams);
        assertEquals(1, wrapCount.get());

        // Second call with the same active password — cache hit, no additional wrap.
        chunkedToXContent(metadata, gatewayParams);
        assertEquals(1, wrapCount.get());

        // Active password changes — cache miss, wrap called again.
        activeId.set("v2");
        chunkedToXContent(metadata, gatewayParams);
        assertEquals(2, wrapCount.get());
    }

    public void testGatewaySerializationFailsLoudlyWhenWrapFails() {
        PekEncryption failing = new PekEncryption() {
            @Override
            public String activePasswordId() {
                throw new IllegalStateException("no active password configured");
            }

            @Override
            public WrappedKey wrap(byte[] plaintext) {
                throw new IllegalStateException("no active password configured");
            }

            @Override
            public byte[] unwrap(byte[] wrapped, String passwordId) {
                return wrapped.clone();
            }
        };

        ToXContent.Params gatewayParams = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", new KeyEntry(randomPlaintextBytes(), 0L)),
            "k1",
            "v1",
            Map.of(),
            failing
        );

        // Wrap failure must propagate — writing empty keys would destroy a recoverable on-disk blob.
        expectThrows(IllegalStateException.class, () -> chunkedToXContent(metadata, gatewayParams));
    }

    public void testDegradedInstancePreservesWrappedBytesForRecovery() throws IOException {
        byte[] keyBytes = randomPlaintextBytes();
        // NO_OP_ENCRYPTION: wrap returns bytes as-is, so wrapped == plaintext here (test only).
        ProjectEncryptionKeyMetadata healthy = new ProjectEncryptionKeyMetadata(
            Map.of("k1", new KeyEntry(keyBytes, 42L)),
            "k1",
            "v1",
            Map.of("handler", "k1"),
            NO_OP_ENCRYPTION
        );

        ToXContent.Params gatewayParams = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        String originalBlob = chunkedToXContent(healthy, gatewayParams);
        assertThat(originalBlob, containsString("\"bytes\""));

        // Parse the blob with a PekEncryption whose unwrap always fails (simulates wrong password).
        PekEncryption wrongPassword = new PekEncryption() {
            @Override
            public String activePasswordId() {
                return "v1";
            }

            @Override
            public WrappedKey wrap(byte[] plaintext) {
                return new WrappedKey("v1", plaintext.clone());
            }

            @Override
            public byte[] unwrap(byte[] wrapped, String passwordId) {
                throw new IllegalStateException("wrong password");
            }
        };

        ProjectEncryptionKeyMetadata.DegradedBlobHolder holder = new ProjectEncryptionKeyMetadata.DegradedBlobHolder();
        ProjectEncryptionKeyMetadata degraded;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, originalBlob)) {
            degraded = ProjectEncryptionKeyMetadata.fromXContent(parser, wrongPassword, holder);
        }
        assertTrue("instance must be degraded after unwrap failure", degraded.isUnwrapFailed());

        // Re-serializing the degraded instance must reproduce the original key material, not empty.
        String repersisted = chunkedToXContent(degraded, gatewayParams);
        assertThat("re-persisted degraded blob must retain active_key_id", repersisted, containsString("\"active_key_id\":\"k1\""));
        assertThat("re-persisted degraded blob must retain password_id", repersisted, containsString("\"password_id\":\"v1\""));
        assertThat("re-persisted degraded blob must retain wrapped bytes, not empty keys", repersisted, containsString("\"bytes\""));
        assertThat("re-persisted degraded blob must not be empty keys", repersisted, not(containsString("\"keys\":{}")));
    }

    public void testDegradedTransportCarriesNoCiphertextAndPeerWithoutBlobWritesEmpty() throws IOException {
        // Ciphertext must not travel over transport — each peer uses its own node-local holder.
        byte[] keyBytes = randomPlaintextBytes();
        ProjectEncryptionKeyMetadata healthy = new ProjectEncryptionKeyMetadata(
            Map.of("k1", new KeyEntry(keyBytes, 42L)),
            "k1",
            "v1",
            Map.of(),
            NO_OP_ENCRYPTION
        );
        ToXContent.Params gatewayParams = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));

        PekEncryption wrongPassword = new PekEncryption() {
            @Override
            public String activePasswordId() {
                return "v1";
            }

            @Override
            public WrappedKey wrap(byte[] plaintext) {
                return new WrappedKey("v1", plaintext.clone());
            }

            @Override
            public byte[] unwrap(byte[] wrapped, String passwordId) {
                throw new IllegalStateException("wrong password");
            }
        };

        // Degrade on the master node.
        ProjectEncryptionKeyMetadata.DegradedBlobHolder masterHolder = new ProjectEncryptionKeyMetadata.DegradedBlobHolder();
        ProjectEncryptionKeyMetadata degraded;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, chunkedToXContent(healthy, gatewayParams))) {
            degraded = ProjectEncryptionKeyMetadata.fromXContent(parser, wrongPassword, masterHolder);
        }
        assertTrue(degraded.isUnwrapFailed());

        // Publish to a peer that has never read a local disk blob (empty holder).
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(ProjectEncryptionKeyMetadata.PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT);
        degraded.writeTo(out);

        ProjectEncryptionKeyMetadata.DegradedBlobHolder peerHolder = new ProjectEncryptionKeyMetadata.DegradedBlobHolder();
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(ProjectEncryptionKeyMetadata.PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT);
        ProjectEncryptionKeyMetadata received = new ProjectEncryptionKeyMetadata(in, wrongPassword, peerHolder);

        assertTrue("degraded flag must survive transport round-trip", received.isUnwrapFailed());

        // Peer has no local blob — writes empty keys (no data loss, just no material to preserve).
        String peerBlob = chunkedToXContent(received, gatewayParams);
        assertThat("peer without local blob writes empty keys", peerBlob, containsString("\"keys\":{}"));
    }

    public void testPeerWithOwnHolderReemitsOwnBlobNotMasters() throws IOException {
        // A peer that already read its own disk blob re-emits that blob even after a degraded master publishes.
        ToXContent.Params gatewayParams = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));

        // Peer pre-populates its holder at startup by reading its own disk blob (healthy, key "pk1").
        // NO_OP_ENCRYPTION.activePasswordId() == "v1" — that is the password_id baked into the on-disk blob.
        ProjectEncryptionKeyMetadata.DegradedBlobHolder peerHolder = new ProjectEncryptionKeyMetadata.DegradedBlobHolder();
        String peerDiskBlob = chunkedToXContent(
            new ProjectEncryptionKeyMetadata(
                Map.of("pk1", new KeyEntry(randomPlaintextBytes(), 10L)),
                "pk1",
                TestPekEncryption.PASSWORD_ID,
                Map.of("myhandler", "pk1"),
                NO_OP_ENCRYPTION
            ),
            gatewayParams
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, peerDiskBlob)) {
            ProjectEncryptionKeyMetadata.fromXContent(parser, NO_OP_ENCRYPTION, peerHolder);
        }
        assertNotNull("holder must be populated after healthy fromXContent", peerHolder.blob);

        // Master degrades and publishes via transport (no ciphertext in the wire bytes).
        ProjectEncryptionKeyMetadata masterDegraded = ProjectEncryptionKeyMetadata.degraded(
            "mk1",
            TestPekEncryption.PASSWORD_ID,
            NO_OP_ENCRYPTION,
            "wrong password on master",
            new ProjectEncryptionKeyMetadata.DegradedBlobHolder()
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(ProjectEncryptionKeyMetadata.PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT);
        masterDegraded.writeTo(out);

        // Peer receives the degraded publish but uses its OWN holder.
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(ProjectEncryptionKeyMetadata.PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT);
        ProjectEncryptionKeyMetadata received = new ProjectEncryptionKeyMetadata(in, NO_OP_ENCRYPTION, peerHolder);
        assertTrue(received.isUnwrapFailed());

        // Peer must re-emit its own pk1 blob, NOT the master's mk1.
        String persistedBlob = chunkedToXContent(received, gatewayParams);
        assertThat("peer re-emits its own key", persistedBlob, containsString("\"pk1\""));
        assertThat("peer blob has wrapped bytes", persistedBlob, containsString("\"bytes\""));
        assertThat("peer blob retains handler_key_ids", persistedBlob, containsString("\"myhandler\""));
        assertThat("peer blob must not contain master key id", persistedBlob, not(containsString("\"mk1\"")));
    }

    public void testDegradedWithoutMaterialWritesEmptyKeys() throws IOException {
        // Disk already had keys:{} (a previously-degraded node wrote nothing) — nothing to preserve.
        ToXContent.Params gatewayParams = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        try (
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"active_key_id\":\"k1\",\"password_id\":\"v1\",\"keys\":{}}")
        ) {
            ProjectEncryptionKeyMetadata degraded = ProjectEncryptionKeyMetadata.fromXContent(
                parser,
                NO_OP_ENCRYPTION,
                new ProjectEncryptionKeyMetadata.DegradedBlobHolder()
            );
            assertTrue("must be degraded when disk had empty keys", degraded.isUnwrapFailed());
            String json = chunkedToXContent(degraded, gatewayParams);
            assertThat("nothing to preserve — must write empty keys", json, containsString("\"keys\":{}"));
        }
    }

    public void testFindRetireableKeyIdsKeyDeactivatedAtRotation() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L), "k2", entry(1_000_000L)),
            "k2",
            PASSWORD_ID,
            Map.of(),
            NO_OP_ENCRYPTION
        );
        assertTrue(metadata.findRetireableKeyIds(500_000L).isEmpty());
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(2_000_000L));
    }
}
