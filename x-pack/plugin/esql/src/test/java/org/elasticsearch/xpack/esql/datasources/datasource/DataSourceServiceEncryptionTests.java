/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link DataSourceService#applyEncryption}, the per-setting encrypt transform. The full
 * putDataSource path (CAS task + cluster-state update) is covered by {@code DataSourceCrudIT}; this file
 * pins the transform in isolation.
 */
public class DataSourceServiceEncryptionTests extends ESTestCase {

    public void testNoServiceWithNoSecretsPassesThrough() {
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("max_retries", new DataSourceSetting(7, false));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), null);

        assertEquals(in.size(), out.size());
        for (var e : in.entrySet()) {
            assertEquals("setting [" + e.getKey() + "] preserved", e.getValue(), out.get(e.getKey()));
        }
    }

    public void testNoServiceWithSecretsIsRejected() {
        // Mandatory encryption: storing a secret with no service available must fail rather than persist
        // plaintext to cluster state.
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("secret_access_key", new DataSourceSetting("AKIA_plaintext", true));

        ElasticsearchStatusException ese = expectThrows(
            ElasticsearchStatusException.class,
            () -> DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), null)
        );
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ese.status());
        assertThat(ese.getMessage(), containsString("no encryption service is available"));
    }

    public void testSecretStringIsEncryptedToCarrier() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        String canary = "AKIA_canary_" + randomAlphaOfLength(8);
        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(canary, true));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals(1, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue("secret flag preserved", result.secret());
        assertTrue("plaintext String replaced by an EncryptedData carrier", result.isEncrypted());
        assertThat(result.rawValue(), instanceOf(EncryptedData.class));
        assertThat(decryptPayload((EncryptedData) result.rawValue()), equalTo(canary));
    }

    public void testNonStringSecretIsEncryptedAndRoundTripsType() {
        // C1: a secret may carry any generic value; the payload is writeGenericValue-serialized so the
        // original type is restored on decrypt.
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        Integer canary = randomInt();
        Map<String, DataSourceSetting> in = Map.of("secret_token", new DataSourceSetting(canary, true));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals(1, encryptCalls.get());
        DataSourceSetting result = out.get("secret_token");
        assertTrue(result.isEncrypted());
        assertThat(decryptPayload((EncryptedData) result.rawValue()), equalTo(canary));
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

    public void testAlreadyEncryptedSettingIsNotDoubleEncrypted() {
        AtomicInteger encryptCalls = new AtomicInteger();
        EncryptionService svc = countingService(encryptCalls);

        EncryptedData preEncrypted = new EncryptedData("upstream-key", new byte[] { 1, 2, 3, 4 });
        Map<String, DataSourceSetting> in = Map.of("secret_access_key", new DataSourceSetting(preEncrypted, true));

        DataSourceSettings out = DataSourceService.applyEncryption("ds-test", new DataSourceSettings(in), svc);

        assertEquals("encrypt not invoked for an already-encrypted setting", 0, encryptCalls.get());
        DataSourceSetting result = out.get("secret_access_key");
        assertTrue(result.isEncrypted());
        assertSame("carrier forwarded by reference", preEncrypted, result.rawValue());
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

        assertEquals("encrypt called once per non-null secret", 2, encryptCalls.get());
        assertEquals("us-east-1", out.get("region").rawValue());
        assertFalse(out.get("region").isEncrypted());
        assertFalse(out.get("endpoint").isEncrypted());
        assertFalse(out.get("max_retries").isEncrypted());
        assertTrue(out.get("access_key").isEncrypted());
        assertTrue(out.get("secret_access_key").isEncrypted());
        assertEquals(ak, decryptPayload((EncryptedData) out.get("access_key").rawValue()));
        assertEquals(sk, decryptPayload((EncryptedData) out.get("secret_access_key").rawValue()));
    }

    /** The counting service's payload is the writeGenericValue blob; restore the original value. */
    private static Object decryptPayload(EncryptedData encrypted) {
        try (StreamInput in = new BytesArray(encrypted.payload()).streamInput()) {
            return in.readGenericValue();
        } catch (IOException e) {
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
