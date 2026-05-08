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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ProjectEncryptionKeyMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.ProjectCustom> {

    private static final String PASSWORD_ID = "v1";

    private static byte[] randomWrappedBytes() {
        // Vary the wrap length by up to 64 extra bytes around the canonical SALT + OVERHEAD + PEK_LENGTH length so the metadata
        // round-trip tests catch any field that accidentally couples to a fixed payload length.
        int min = PasswordBasedEncryption.SALT_LENGTH_BYTES + AesGcm.OVERHEAD_BYTES + PasswordBasedEncryption.PEK_LENGTH_BYTES;
        return randomByteArrayOfLength(randomIntBetween(min, min + 64));
    }

    private record RandomEntries(Map<String, KeyEntry> entries, String activeKeyId) {}

    private static RandomEntries randomEntriesWithLastActive(int count) {
        // Generate entries with strictly-increasing generatedAt so the last-inserted entry is the newest.
        Map<String, KeyEntry> entries = new HashMap<>();
        long ts = randomLongBetween(0L, Long.MAX_VALUE - count);
        String lastId = null;
        for (int i = 0; i < count; i++) {
            lastId = ProjectEncryptionKeyMetadata.generateKeyId();
            entries.put(lastId, new KeyEntry(randomWrappedBytes(), ts++));
        }
        return new RandomEntries(entries, lastId);
    }

    private static ProjectEncryptionKeyMetadata randomPekMetadata() {
        RandomEntries r = randomEntriesWithLastActive(randomIntBetween(1, 5));
        return new ProjectEncryptionKeyMetadata(r.entries(), r.activeKeyId(), PASSWORD_ID);
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
        // Pick mutation 0 (add new active) when no non-active entry exists to mutate.
        boolean addNew = entries.size() == 1 || randomBoolean();
        if (addNew && activeTs < Long.MAX_VALUE) {
            // Add a new entry strictly newer than the current active, and make it active.
            String newId = ProjectEncryptionKeyMetadata.generateKeyId();
            long newTs = randomLongBetween(activeTs + 1, Long.MAX_VALUE);
            entries.put(newId, new KeyEntry(randomWrappedBytes(), newTs));
            return new ProjectEncryptionKeyMetadata(entries, newId, pek.getPasswordId());
        }
        // Mutate a non-active entry's timestamp (active stays newest by construction).
        String id = randomValueOtherThan(activeId, () -> randomFrom(entries.keySet()));
        KeyEntry old = entries.get(id);
        long newTs = randomValueOtherThan(old.generatedAt(), () -> randomLongBetween(0L, activeTs - 1));
        entries.put(id, new KeyEntry(old.bytes(), newTs));
        return new ProjectEncryptionKeyMetadata(entries, activeId, pek.getPasswordId());
    }

    @Override
    protected Metadata.ProjectCustom doParseInstance(XContentParser parser) throws IOException {
        return ProjectEncryptionKeyMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return ProjectEncryptionKeyMetadata::new;
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
                    ProjectEncryptionKeyMetadata::new
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

    public void testApiContextRedactsWrappedBytesButKeepsOperationalFields() throws IOException {
        ProjectEncryptionKeyMetadata metadata = randomPekMetadata();
        String apiJson = chunkedToXContent(metadata, new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, "API")));
        String gatewayJson = chunkedToXContent(metadata, new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, "GATEWAY")));

        // Operational metadata the control plane reads must remain visible under API.
        assertThat(apiJson, containsString("\"active_key_id\":\"" + metadata.getActiveKeyId() + "\""));
        assertThat(apiJson, containsString("\"password_id\":\"" + metadata.getPasswordId() + "\""));
        assertThat(apiJson, containsString("\"generated_at\""));
        // Wrapped key bytes must be omitted under API but present under GATEWAY (so disk round-trip works).
        assertThat("API context must not emit a 'bytes' field for any key entry", apiJson, not(containsString("\"bytes\"")));
        assertThat(
            "GATEWAY context must still emit 'bytes' so on-disk reload can reconstruct the wrapped key",
            gatewayJson,
            containsString("\"bytes\"")
        );
        // None of the wrapped key payloads should leak verbatim under API context.
        for (KeyEntry entry : metadata.getKeys().values()) {
            String base64 = java.util.Base64.getEncoder().encodeToString(entry.bytes());
            assertThat("API context must not leak wrapped key bytes (base64)", apiJson, not(containsString(base64)));
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

    public void testGetEncryptedKeyReturnsEncryptedDataUnderPasswordId() {
        RandomEntries r = randomEntriesWithLastActive(3);
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(r.entries(), r.activeKeyId(), PASSWORD_ID);
        for (var entry : r.entries().entrySet()) {
            EncryptedData encrypted = metadata.getEncryptedKey(entry.getKey());
            assertNotNull(encrypted);
            assertEquals(PASSWORD_ID, encrypted.keyId());
            assertArrayEquals(entry.getValue().bytes(), encrypted.payload());
        }
        assertNull(metadata.getEncryptedKey("nonexistent"));
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
            () -> new ProjectEncryptionKeyMetadata(Map.of(keyId, new KeyEntry(randomWrappedBytes(), 0L)), "nonexistent", PASSWORD_ID)
        );
    }

    public void testEmptyEntriesMap() {
        expectThrows(IllegalArgumentException.class, () -> new ProjectEncryptionKeyMetadata(Map.of(), "any", PASSWORD_ID));
    }

    public void testNullPasswordIdRejected() {
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        expectThrows(
            NullPointerException.class,
            () -> new ProjectEncryptionKeyMetadata(Map.of(keyId, new KeyEntry(randomWrappedBytes(), 0L)), keyId, null)
        );
    }

    public void testFromXContentMissingActiveKeyId() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"password_id\":\"v1\",\"keys\":{\"abc\":{\"bytes\":\"AAAA\",\"generated_at\":0}}}"
            )
        ) {
            expectThrows(IllegalArgumentException.class, () -> ProjectEncryptionKeyMetadata.fromXContent(parser));
        }
    }

    public void testFromXContentMissingKeys() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"active_key_id\":\"abc\",\"password_id\":\"v1\"}")) {
            expectThrows(IllegalArgumentException.class, () -> ProjectEncryptionKeyMetadata.fromXContent(parser));
        }
    }

    public void testFromXContentMissingPasswordIdDefaultsToEmpty() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"active_key_id\":\"abc\",\"keys\":{\"abc\":{\"bytes\":\"AAAA\",\"generated_at\":0}}}"
            )
        ) {
            ProjectEncryptionKeyMetadata reconstructed = (ProjectEncryptionKeyMetadata) ProjectEncryptionKeyMetadata.fromXContent(parser);
            assertEquals("", reconstructed.getPasswordId());
            assertEquals("abc", reconstructed.getActiveKeyId());
        }
    }

    public void testReadFromPreAtRestWireFormatDefaultsPasswordIdToEmpty() throws IOException {
        RandomEntries r = randomEntriesWithLastActive(randomIntBetween(1, 4));
        Map<String, String> handlerKeyIds = randomBoolean() ? Map.of() : Map.of("handler-A", r.activeKeyId());

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(ProjectEncryptionKeyMetadata.PRIMARY_ENCRYPTION_KEY_ROTATION);
        out.writeMap(r.entries(), StreamOutput::writeString, (o, e) -> e.writeTo(o));
        out.writeString(r.activeKeyId());
        out.writeMap(handlerKeyIds, StreamOutput::writeString, StreamOutput::writeString);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(ProjectEncryptionKeyMetadata.PRIMARY_ENCRYPTION_KEY_ROTATION);
        ProjectEncryptionKeyMetadata read = new ProjectEncryptionKeyMetadata(in);

        assertEquals(r.activeKeyId(), read.getActiveKeyId());
        assertEquals("", read.getPasswordId());
        assertEquals(r.entries().keySet(), read.getKeys().keySet());
        assertEquals(handlerKeyIds, read.getHandlerKeyIds());
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomWrappedBytes(), generatedAt);
    }

    public void testFindRetireableKeyIdsExcludesActive() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L), "k2", entry(100L)),
            "k2",
            PASSWORD_ID
        );
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(Long.MAX_VALUE));
    }

    public void testFindRetireableKeyIdsUsesDeactivationTimeNotGenerationTime() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(100L), "k2", entry(400L), "k3", entry(500L)),
            "k3",
            PASSWORD_ID
        );
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(450L));
    }

    public void testFindRetireableKeyIdsReturnsEmptyWhenNoNonActiveKeys() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("only", entry(100L)), "only", PASSWORD_ID);
        assertTrue(metadata.findRetireableKeyIds(Long.MAX_VALUE).isEmpty());
    }

    public void testFindRetireableKeyIdsKeyDeactivatedAtRotation() {
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(0L), "k2", entry(1_000_000L)),
            "k2",
            PASSWORD_ID
        );
        assertTrue(metadata.findRetireableKeyIds(500_000L).isEmpty());
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(2_000_000L));
    }
}
