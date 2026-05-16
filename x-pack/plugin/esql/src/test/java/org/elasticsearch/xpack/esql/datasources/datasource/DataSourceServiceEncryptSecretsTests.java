/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
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
 * Unit tests for {@link DataSourceService#encryptSecrets}. The full putDataSource path (gate +
 * cluster-state task + 503 negative case) is covered by {@code DataSourceCrudIT} and
 * {@code DataSourceEncryptionRequiredIT}; this file pins the per-setting encryption transform.
 */
public class DataSourceServiceEncryptSecretsTests extends ESTestCase {

    public void testNonSecretSettingsPassThroughUnchanged() {
        EncryptionService never = countingService();
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("max_retries", new DataSourceSetting(7, false));
        in.put("use_path_style", new DataSourceSetting(true, false));

        Map<String, DataSourceSetting> out = DataSourceService.encryptSecrets(in, never);

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

        Map<String, DataSourceSetting> out = DataSourceService.encryptSecrets(in, svc);

        assertEquals(1, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue("secret flag preserved", result.secret());
        assertThat("plaintext String replaced by carrier", result.rawValue(), instanceOf(EncryptedData.class));
        EncryptedData ed = (EncryptedData) result.rawValue();
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
        Map<String, DataSourceSetting> out = DataSourceService.encryptSecrets(in, svc);

        assertEquals("encrypt() not invoked for null secret", 0, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue(result.secret());
        assertNull(result.rawValue());
    }

    public void testAlreadyEncryptedCarrierPassesThrough() {
        // Defensive: if upstream ever hands us a pre-encrypted carrier (e.g. replay paths), don't
        // double-encrypt. Forward unchanged.
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

        EncryptedData preEncrypted = new EncryptedData("upstream-key", "AKIA_old".getBytes(StandardCharsets.UTF_8));
        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(preEncrypted, true));

        Map<String, DataSourceSetting> out = DataSourceService.encryptSecrets(in, svc);

        assertEquals("encrypt() not invoked when value is already a carrier", 0, encryptCalls.get());
        assertSame(preEncrypted, out.get("secret_access_key").rawValue());
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

        Map<String, DataSourceSetting> out = DataSourceService.encryptSecrets(in, svc);

        assertEquals("encrypt() called once per secret with a String value", 2, encryptCalls.get());

        assertEquals("us-east-1", out.get("region").rawValue());
        assertEquals("https://example.test", out.get("endpoint").rawValue());
        assertEquals(7, out.get("max_retries").rawValue());

        assertThat(out.get("access_key").rawValue(), instanceOf(EncryptedData.class));
        assertThat(out.get("secret_access_key").rawValue(), instanceOf(EncryptedData.class));
        assertEquals(ak, new String(((EncryptedData) out.get("access_key").rawValue()).payload(), StandardCharsets.UTF_8));
        assertEquals(sk, new String(((EncryptedData) out.get("secret_access_key").rawValue()).payload(), StandardCharsets.UTF_8));
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
