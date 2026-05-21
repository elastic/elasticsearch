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
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link DataSourceEncryption#apply}. The full putDataSource path (gate +
 * cluster-state task + 503 negative case) is covered by {@code DataSourceCrudIT} and
 * {@code DataSourceWithoutEncryptionIT}; this file pins the per-setting encryption transform.
 */
public class DataSourceEncryptionTests extends ESTestCase {

    public void testNonSecretSettingsPassThroughUnchanged() {
        EncryptionService never = countingService();
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("max_retries", new DataSourceSetting(7, false));
        in.put("use_path_style", new DataSourceSetting(true, false));

        DataSourceSettings out = new DataSourceEncryption(never).apply("ds-test", new DataSourceSettings(in));

        assertEquals(in.size(), out.size());
        for (var e : in.entrySet()) {
            assertEquals("setting [" + e.getKey() + "] preserved", e.getValue(), out.get(e.getKey()));
        }
    }

    public void testSecretStringIsEncryptedToCarrier() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                encryptCalls.incrementAndGet();
                return new EncryptedData("test-key", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        };

        String canary = "AKIA_canary_" + randomAlphaOfLength(8);
        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(canary, true));

        DataSourceSettings out = new DataSourceEncryption(svc).apply("ds-test", new DataSourceSettings(in));

        assertEquals(1, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue("secret flag preserved", result.secret());
        assertEquals(
            "encryption format is the discriminator readers branch on",
            DataSourceSetting.EncryptionFormat.V1,
            result.encryption()
        );
        assertTrue("isEncryptedBlob agrees with the format enum", result.isEncryptedBlob());
        assertThat("plaintext String replaced by encrypted byte[] blob", result.rawValue(), instanceOf(byte[].class));
        byte[] blob = (byte[]) result.rawValue();
        EncryptedData ed = decodeBlob(blob);
        assertThat(new String(ed.payload(), StandardCharsets.UTF_8), equalTo(canary));
    }

    public void testNullSecretValueIsPreservedWithoutCallingEncrypt() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                encryptCalls.incrementAndGet();
                return new EncryptedData("test-key", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        };

        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(null, true));
        DataSourceSettings out = new DataSourceEncryption(svc).apply("ds-test", new DataSourceSettings(in));

        assertEquals("encrypt() not invoked for null secret", 0, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue(result.secret());
        assertNull(result.rawValue());
    }

    public void testAlreadyEncryptedV1CarrierPassesThrough() {
        // Defensive: if upstream ever hands us a setting that is already marked V1 (e.g. replay paths
        // where the cluster state already contains the encrypted carrier), don't double-encrypt.
        // Forward unchanged with the V1 marker intact.
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                encryptCalls.incrementAndGet();
                return new EncryptedData("would-double-encrypt", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        };

        byte[] preEncryptedBlob = encodeBlob(new EncryptedData("upstream-key", "AKIA_old".getBytes(StandardCharsets.UTF_8)));
        Map<String, DataSourceSetting> in = Map.of(
            "secret_access_key",
            new DataSourceSetting(preEncryptedBlob, true, DataSourceSetting.EncryptionFormat.V1)
        );

        DataSourceSettings out = new DataSourceEncryption(svc).apply("ds-test", new DataSourceSettings(in));

        assertEquals("encrypt() not invoked when the setting is already marked V1", 0, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertEquals("V1 marker preserved by pass-through", DataSourceSetting.EncryptionFormat.V1, result.encryption());
        assertSame("ciphertext blob forwarded by reference", preEncryptedBlob, result.rawValue());
    }

    public void testMixedSettingsEncryptOnlyTheSecrets() {
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

        DataSourceSettings out = new DataSourceEncryption(svc).apply("ds-test", new DataSourceSettings(in));

        assertEquals("encrypt() called once per secret with a String value", 2, encryptCalls.get());

        assertEquals("us-east-1", out.get("region").rawValue());
        assertEquals("https://example.test", out.get("endpoint").rawValue());
        assertEquals(7, out.get("max_retries").rawValue());
        assertEquals(
            "non-secret region keeps EncryptionFormat.NONE",
            DataSourceSetting.EncryptionFormat.NONE,
            out.get("region").encryption()
        );
        assertEquals(
            "non-secret endpoint keeps EncryptionFormat.NONE",
            DataSourceSetting.EncryptionFormat.NONE,
            out.get("endpoint").encryption()
        );
        assertEquals(
            "non-secret max_retries keeps EncryptionFormat.NONE",
            DataSourceSetting.EncryptionFormat.NONE,
            out.get("max_retries").encryption()
        );

        assertThat(out.get("access_key").rawValue(), instanceOf(byte[].class));
        assertThat(out.get("secret_access_key").rawValue(), instanceOf(byte[].class));
        assertEquals("encrypted access_key is marked V1", DataSourceSetting.EncryptionFormat.V1, out.get("access_key").encryption());
        assertEquals(
            "encrypted secret_access_key is marked V1",
            DataSourceSetting.EncryptionFormat.V1,
            out.get("secret_access_key").encryption()
        );
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

    private static EncryptionService countingService() {
        return countingService(new AtomicInteger());
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
