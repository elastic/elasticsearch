/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DataSourceEncryptedDataHandlerTests extends ESTestCase {

    private static final String ACTIVE_KEY = "key-2";
    private static final String STALE_KEY = "key-1";

    /** Wraps under the active key id; decrypt just returns the payload (the identity stub is keyless). */
    private static final EncryptionService SERVICE = new EncryptionService() {
        @Override
        public EncryptedData encrypt(byte[] bytes) {
            return new EncryptedData(ACTIVE_KEY, bytes.clone());
        }

        @Override
        public byte[] decrypt(EncryptedData encryptedData) {
            return encryptedData.payload();
        }
    };

    private final DataSourceEncryptedDataHandler handler = new DataSourceEncryptedDataHandler();

    public void testCustomNameMatchesMetadataType() {
        assertEquals(DataSourceMetadata.TYPE, handler.customName());
    }

    public void testNullAndEmptyAreReturnedAsIs() {
        assertNull(handler.reEncrypt(null, SERVICE, ACTIVE_KEY));
        DataSourceMetadata empty = DataSourceMetadata.EMPTY;
        assertSame(empty, handler.reEncrypt(empty, SERVICE, ACTIVE_KEY));
    }

    public void testStaleKeySecretIsReEncryptedUnderActiveKey() {
        byte[] payload = "AKIA_secret".getBytes(StandardCharsets.UTF_8);
        DataSource ds = new DataSource(
            "s3",
            "s3",
            null,
            Map.of(
                "region",
                new DataSourceSetting("us-east-1", false),
                "access_key",
                new DataSourceSetting(new EncryptedData(STALE_KEY, payload), true)
            )
        );
        DataSourceMetadata before = new DataSourceMetadata(Map.of("s3", ds));

        DataSourceMetadata after = handler.reEncrypt(before, SERVICE, ACTIVE_KEY);

        assertNotSame(before, after);
        DataSourceSetting reKeyed = after.get("s3").settings().get("access_key");
        assertTrue(reKeyed.isEncrypted());
        EncryptedData carrier = (EncryptedData) reKeyed.rawValue();
        assertEquals("re-encrypted under the active key", ACTIVE_KEY, carrier.keyId());
        assertArrayEquals("payload bytes preserved verbatim", payload, carrier.payload());
        // non-secret untouched
        assertEquals("us-east-1", after.get("s3").settings().get("region").nonSecretValue());
    }

    public void testSecretAlreadyOnActiveKeyIsUntouched() {
        DataSource ds = new DataSource(
            "s3",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting(new EncryptedData(ACTIVE_KEY, new byte[] { 1, 2, 3 }), true))
        );
        DataSourceMetadata before = new DataSourceMetadata(Map.of("s3", ds));

        DataSourceMetadata after = handler.reEncrypt(before, SERVICE, ACTIVE_KEY);

        assertSame("no secret needed re-encryption, so the custom is returned by reference", before, after);
    }

    public void testMixedDataSourcesOnlyRewriteStaleOnes() {
        DataSource stale = new DataSource(
            "stale",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting(new EncryptedData(STALE_KEY, new byte[] { 9 }), true))
        );
        DataSource fresh = new DataSource(
            "fresh",
            "s3",
            null,
            Map.of("access_key", new DataSourceSetting(new EncryptedData(ACTIVE_KEY, new byte[] { 8 }), true))
        );
        DataSourceMetadata before = new DataSourceMetadata(Map.of("stale", stale, "fresh", fresh));

        DataSourceMetadata after = handler.reEncrypt(before, SERVICE, ACTIVE_KEY);

        assertNotSame(before, after);
        assertEquals(ACTIVE_KEY, ((EncryptedData) after.get("stale").settings().get("access_key").rawValue()).keyId());
        assertSame("the already-active data source is forwarded by reference", fresh, after.get("fresh"));
    }
}
