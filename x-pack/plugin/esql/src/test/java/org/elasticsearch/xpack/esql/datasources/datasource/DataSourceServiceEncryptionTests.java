/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.cluster.metadata.DataSourceSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link DataSourceService#applyEncryption}, the per-setting encrypt-or-fallback
 * transform. The full putDataSource path (CAS task + cluster-state update) is covered by
 * {@code DataSourceCrudIT}; this file pins the transform in isolation.
 */
public class DataSourceServiceEncryptionTests extends ESTestCase {

    public void testNoServiceLeavesSettingsUnchanged() {
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("secret_access_key", new DataSourceSetting("AKIA_plaintext", true));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), null);

        assertEquals(in.size(), out.size());
        for (var e : in.entrySet()) {
            assertEquals("setting [" + e.getKey() + "] preserved", e.getValue(), out.get(e.getKey()));
        }
    }

    public void testSecretStringIsEncryptedToV1Carrier() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        String canary = "AKIA_canary_" + randomAlphaOfLength(8);
        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(canary, true));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals(1, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue("secret flag preserved", result.secret());
        assertEquals("discriminator is V1", DataSourceSetting.EncryptionFormat.V1, result.encryption());
        assertTrue("isEncryptedBlob agrees with the format enum", result.isEncryptedBlob());
        assertThat("plaintext String replaced by encrypted byte[] blob", result.rawValue(), instanceOf(byte[].class));
        EncryptedData ed = decodeBlob((byte[]) result.rawValue());
        assertThat(new String(ed.payload(), StandardCharsets.UTF_8), equalTo(canary));
    }

    public void testNullSecretValueIsPreservedWithoutCallingEncrypt() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(null, true));
        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals("encrypt not invoked for null secret", 0, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue(result.secret());
        assertNull(result.rawValue());
    }

    public void testAlreadyV1SettingIsNotDoubleEncrypted() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        byte[] preEncrypted = encodeBlob(new EncryptedData("upstream-key", "AKIA_old".getBytes(StandardCharsets.UTF_8)));
        Map<String, DataSourceSetting> in = Map.of(
            "secret_access_key",
            new DataSourceSetting(preEncrypted, true, DataSourceSetting.EncryptionFormat.V1)
        );

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals("encrypt not invoked for a setting already marked V1", 0, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertEquals(DataSourceSetting.EncryptionFormat.V1, result.encryption());
        assertSame("ciphertext blob forwarded by reference", preEncrypted, result.rawValue());
    }

    public void testMixedSettingsEncryptOnlyStringSecrets() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        String ak = "ak_" + randomAlphaOfLength(8);
        String sk = "sk_" + randomAlphaOfLength(8);
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("endpoint", new DataSourceSetting("https://example.test", false));
        in.put("access_key", new DataSourceSetting(ak, true));
        in.put("secret_access_key", new DataSourceSetting(sk, true));
        in.put("max_retries", new DataSourceSetting(7, false));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals("encrypt called once per String secret", 2, encryptCalls.get());
        assertEquals("us-east-1", out.get("region").rawValue());
        assertEquals(DataSourceSetting.EncryptionFormat.NONE, out.get("region").encryption());
        assertEquals(DataSourceSetting.EncryptionFormat.NONE, out.get("endpoint").encryption());
        assertEquals(DataSourceSetting.EncryptionFormat.NONE, out.get("max_retries").encryption());
        assertEquals(DataSourceSetting.EncryptionFormat.V1, out.get("access_key").encryption());
        assertEquals(DataSourceSetting.EncryptionFormat.V1, out.get("secret_access_key").encryption());
        assertEquals(ak, new String(decodeBlob((byte[]) out.get("access_key").rawValue()).payload(), StandardCharsets.UTF_8));
        assertEquals(sk, new String(decodeBlob((byte[]) out.get("secret_access_key").rawValue()).payload(), StandardCharsets.UTF_8));
    }

    private static byte[] encodeBlob(EncryptedData encrypted) {
        try {
            org.elasticsearch.common.io.stream.BytesStreamOutput out = new org.elasticsearch.common.io.stream.BytesStreamOutput();
            encrypted.writeTo(out);
            return org.elasticsearch.common.bytes.BytesReference.toBytes(out.bytes());
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static EncryptedData decodeBlob(byte[] blob) {
        try (org.elasticsearch.common.io.stream.StreamInput in = new org.elasticsearch.common.bytes.BytesArray(blob).streamInput()) {
            return new EncryptedData(in);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static EncryptionService countingService(AtomicInteger counter) {
        return new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                counter.incrementAndGet();
                return new EncryptedData("test-key", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        };
    }
}
