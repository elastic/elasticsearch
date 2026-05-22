/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.security.crypto.PrimaryEncryptionKeyMetadata.KeyEntry;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

public class PrimaryEncryptionKeyMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.ProjectCustom> {

    private static byte[] randomKeyBytes() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static Map<String, KeyEntry> randomEntriesWithLastActive(int count, String[] activeIdHolder) {
        // Generate entries with strictly-increasing generatedAt so the last-inserted entry is the newest.
        Map<String, KeyEntry> entries = new HashMap<>();
        long ts = randomLongBetween(0L, Long.MAX_VALUE - count);
        String lastId = null;
        for (int i = 0; i < count; i++) {
            lastId = PrimaryEncryptionKeyMetadata.generateKeyId();
            entries.put(lastId, new KeyEntry(randomKeyBytes(), ts++));
        }
        activeIdHolder[0] = lastId;
        return entries;
    }

    private static PrimaryEncryptionKeyMetadata randomPekMetadata() {
        String[] activeHolder = new String[1];
        Map<String, KeyEntry> entries = randomEntriesWithLastActive(randomIntBetween(1, 5), activeHolder);
        return new PrimaryEncryptionKeyMetadata(entries, activeHolder[0]);
    }

    @Override
    protected Metadata.ProjectCustom createTestInstance() {
        return randomPekMetadata();
    }

    @Override
    protected Metadata.ProjectCustom mutateInstance(Metadata.ProjectCustom instance) {
        PrimaryEncryptionKeyMetadata pek = (PrimaryEncryptionKeyMetadata) instance;
        Map<String, KeyEntry> entries = new HashMap<>(pek.getKeys());
        String activeId = pek.getActiveKeyId();
        long activeTs = entries.get(activeId).generatedAt();
        // Pick mutation 0 (add new active) when no non-active entry exists to mutate.
        boolean addNew = entries.size() == 1 || randomBoolean();
        if (addNew && activeTs < Long.MAX_VALUE) {
            // Add a new entry strictly newer than the current active, and make it active.
            String newId = PrimaryEncryptionKeyMetadata.generateKeyId();
            long newTs = randomLongBetween(activeTs + 1, Long.MAX_VALUE);
            entries.put(newId, new KeyEntry(randomKeyBytes(), newTs));
            return new PrimaryEncryptionKeyMetadata(entries, newId);
        }
        // Mutate a non-active entry's timestamp (active stays newest by construction).
        String id = randomValueOtherThan(activeId, () -> randomFrom(entries.keySet()));
        KeyEntry old = entries.get(id);
        long newTs = randomValueOtherThan(old.generatedAt(), () -> randomLongBetween(0L, activeTs - 1));
        entries.put(id, new KeyEntry(old.bytes(), newTs));
        return new PrimaryEncryptionKeyMetadata(entries, activeId);
    }

    @Override
    protected Metadata.ProjectCustom doParseInstance(XContentParser parser) throws IOException {
        return PrimaryEncryptionKeyMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return PrimaryEncryptionKeyMetadata::new;
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.ProjectCustom>> diffReader() {
        return PrimaryEncryptionKeyMetadata::readDiffFrom;
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
                    PrimaryEncryptionKeyMetadata.TYPE,
                    PrimaryEncryptionKeyMetadata::new
                )
            )
        );
    }

    public void testContextIsGatewayOnly() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        assertEquals(EnumSet.of(Metadata.XContentContext.GATEWAY), metadata.context());
    }

    public void testToSecretKey() {
        String[] activeHolder = new String[1];
        Map<String, KeyEntry> entries = randomEntriesWithLastActive(3, activeHolder);
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(entries, activeHolder[0]);
        for (var entry : entries.entrySet()) {
            SecretKey key = metadata.toSecretKey(entry.getKey());
            assertNotNull(key);
            assertEquals("AES", key.getAlgorithm());
            assertArrayEquals(entry.getValue().bytes(), key.getEncoded());
        }
        assertNull(metadata.toSecretKey("nonexistent"));
        // No-arg overload returns the active key.
        assertArrayEquals(metadata.toSecretKey(activeHolder[0]).getEncoded(), metadata.toSecretKey().getEncoded());
    }

    public void testGetKeyBytesReturnsDefensiveCopy() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        String keyId = metadata.getActiveKeyId();
        byte[] keyBytes1 = metadata.getKeyBytes(keyId);
        byte[] keyBytes2 = metadata.getKeyBytes(keyId);
        assertArrayEquals(keyBytes1, keyBytes2);
        keyBytes1[0] = (byte) ~keyBytes1[0];
        assertNotEquals(keyBytes1[0], metadata.getKeyBytes(keyId)[0]);
    }

    public void testGetKeyBytesForMissingKeyId() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        assertNull(metadata.getKeyBytes("nonexistent"));
    }

    public void testGetGeneratedAtForMissingKeyId() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        assertEquals(0L, metadata.getGeneratedAt("nonexistent"));
    }

    public void testToStringDoesNotLeakKey() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        String str = metadata.toString();
        assertFalse(str.contains(java.util.Base64.getEncoder().encodeToString(metadata.getKeyBytes(metadata.getActiveKeyId()))));
        assertTrue(str.contains("[keys secret]"));
    }

    public void testInvalidKeyLength() {
        expectThrows(IllegalArgumentException.class, () -> new KeyEntry(new byte[16], 0L));
    }

    public void testActiveKeyIdNotInEntries() {
        String keyId = PrimaryEncryptionKeyMetadata.generateKeyId();
        expectThrows(
            IllegalArgumentException.class,
            () -> new PrimaryEncryptionKeyMetadata(Map.of(keyId, new KeyEntry(randomKeyBytes(), 0L)), "nonexistent")
        );
    }

    public void testEmptyEntriesMap() {
        expectThrows(IllegalArgumentException.class, () -> new PrimaryEncryptionKeyMetadata(Map.of(), "any"));
    }

    public void testFromXContentMissingActiveKeyId() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"keys\": {\"abc\": {\"bytes\": \"AAAA\"}}}")) {
            expectThrows(IllegalArgumentException.class, () -> PrimaryEncryptionKeyMetadata.fromXContent(parser));
        }
    }

    public void testFromXContentMissingKeys() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"active_key_id\": \"abc\"}")) {
            expectThrows(IllegalArgumentException.class, () -> PrimaryEncryptionKeyMetadata.fromXContent(parser));
        }
    }

    private static KeyEntry entry(long generatedAt) {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return new KeyEntry(keyBytes, generatedAt);
    }

    public void testFindRetireableKeyIdsExcludesActive() {
        // Even with active.generatedAt below the cutoff, active is never retired.
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(50L), "k2", entry(100L)), "k2");
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(Long.MAX_VALUE));
    }

    public void testFindRetireableKeyIdsUsesDeactivationTimeNotGenerationTime() {
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("k1", entry(100L), "k2", entry(400L), "k3", entry(500L)),
            "k3"
        );
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(450L));
    }

    public void testFindRetireableKeyIdsReturnsEmptyWhenNoNonActiveKeys() {
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("only", entry(100L)), "only");
        assertTrue(metadata.findRetireableKeyIds(Long.MAX_VALUE).isEmpty());
    }

    public void testFindRetireableKeyIdsKeyDeactivatedAtRotation() {
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(0L), "k2", entry(1_000_000L)), "k2");
        assertTrue(metadata.findRetireableKeyIds(500_000L).isEmpty());
        assertEquals(Set.of("k1"), metadata.findRetireableKeyIds(2_000_000L));
    }
}
