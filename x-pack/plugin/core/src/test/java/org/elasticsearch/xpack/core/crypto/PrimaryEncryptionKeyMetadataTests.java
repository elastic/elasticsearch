/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.SecretKey;

public class PrimaryEncryptionKeyMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.ProjectCustom> {

    private static Map<String, byte[]> randomKeys(int count) {
        Map<String, byte[]> keys = new HashMap<>();
        for (int i = 0; i < count; i++) {
            byte[] keyBytes = new byte[32];
            random().nextBytes(keyBytes);
            keys.put(PrimaryEncryptionKeyMetadata.generateKeyId(), keyBytes);
        }
        return keys;
    }

    private static PrimaryEncryptionKeyMetadata randomPekMetadata() {
        Map<String, byte[]> keys = randomKeys(randomIntBetween(1, 5));
        String activeKeyId = randomFrom(keys.keySet());
        return new PrimaryEncryptionKeyMetadata(keys, activeKeyId);
    }

    @Override
    protected Metadata.ProjectCustom createTestInstance() {
        return randomPekMetadata();
    }

    @Override
    protected Metadata.ProjectCustom mutateInstance(Metadata.ProjectCustom instance) {
        PrimaryEncryptionKeyMetadata pek = (PrimaryEncryptionKeyMetadata) instance;
        Map<String, byte[]> newKeys = new HashMap<>(pek.getKeys());
        if (randomBoolean() && newKeys.size() > 1) {
            String keyToRemove = randomValueOtherThan(pek.getActiveKeyId(), () -> randomFrom(newKeys.keySet()));
            newKeys.remove(keyToRemove);
            return new PrimaryEncryptionKeyMetadata(newKeys, pek.getActiveKeyId());
        } else {
            byte[] newKeyBytes = new byte[32];
            random().nextBytes(newKeyBytes);
            String newKeyId = PrimaryEncryptionKeyMetadata.generateKeyId();
            newKeys.put(newKeyId, newKeyBytes);
            return new PrimaryEncryptionKeyMetadata(newKeys, newKeyId);
        }
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

    public void testToSecretKeyReturnsActive() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        SecretKey key = metadata.toSecretKey();
        assertEquals("AES", key.getAlgorithm());
        assertEquals(32, key.getEncoded().length);
        assertArrayEquals(metadata.getKeyBytes(metadata.getActiveKeyId()), key.getEncoded());
    }

    public void testToSecretKeyByKeyId() {
        Map<String, byte[]> keys = randomKeys(3);
        String activeKeyId = randomFrom(keys.keySet());
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(keys, activeKeyId);
        for (var entry : keys.entrySet()) {
            SecretKey key = metadata.toSecretKey(entry.getKey());
            assertNotNull(key);
            assertEquals("AES", key.getAlgorithm());
            assertArrayEquals(entry.getValue(), key.getEncoded());
        }
        assertNull(metadata.toSecretKey("nonexistent"));
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

    public void testToStringDoesNotLeakKey() {
        PrimaryEncryptionKeyMetadata metadata = randomPekMetadata();
        String str = metadata.toString();
        assertFalse(str.contains(java.util.Base64.getEncoder().encodeToString(metadata.getKeyBytes(metadata.getActiveKeyId()))));
        assertTrue(str.contains("[keys secret]"));
    }

    public void testInvalidKeyLength() {
        String keyId = "abcdef0123456789";
        expectThrows(IllegalArgumentException.class, () -> new PrimaryEncryptionKeyMetadata(Map.of(keyId, new byte[16]), keyId));
    }

    public void testActiveKeyIdNotInKeys() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        String keyId = PrimaryEncryptionKeyMetadata.generateKeyId();
        expectThrows(IllegalArgumentException.class, () -> new PrimaryEncryptionKeyMetadata(Map.of(keyId, keyBytes), "nonexistent"));
    }

    public void testEmptyKeysMap() {
        expectThrows(IllegalArgumentException.class, () -> new PrimaryEncryptionKeyMetadata(Map.of(), "any"));
    }

    public void testFromXContentMissingActiveKeyId() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"keys\": {\"abc\": \"AAAA\"}}")) {
            expectThrows(IllegalArgumentException.class, () -> PrimaryEncryptionKeyMetadata.fromXContent(parser));
        }
    }

    public void testFromXContentMissingKeys() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"active_key_id\": \"abc\"}")) {
            expectThrows(IllegalArgumentException.class, () -> PrimaryEncryptionKeyMetadata.fromXContent(parser));
        }
    }
}
