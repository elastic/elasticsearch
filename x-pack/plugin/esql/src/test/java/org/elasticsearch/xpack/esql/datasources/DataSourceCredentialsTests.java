/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DataSourceCredentialsTests extends ESTestCase {

    /** Identity stub for tests — wraps and unwraps the input bytes verbatim under a fixed key id. */
    private static final EncryptionService IDENTITY = new EncryptionService() {
        @Override
        public EncryptedData encrypt(byte[] bytes) {
            return new EncryptedData("test-key", bytes.clone());
        }

        @Override
        public byte[] decrypt(EncryptedData encryptedData) {
            return encryptedData.payload();
        }
    };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        DataSourceCredentials.initialize(IDENTITY);
    }

    public void testNullMapPassesThrough() {
        assertNull(DataSourceCredentials.decryptInPlace(null));
    }

    public void testEmptyMapReturnsCopy() {
        Map<String, Object> empty = Map.of();
        Map<String, Object> out = DataSourceCredentials.decryptInPlace(empty);
        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    public void testNonSecretsPreserved() {
        Map<String, Object> input = new HashMap<>();
        input.put("region", "us-east-1");
        input.put("max_retries", 7);
        input.put("use_path_style", true);
        input.put("optional_label", null);

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertEquals("us-east-1", out.get("region"));
        assertEquals(7, out.get("max_retries"));
        assertEquals(true, out.get("use_path_style"));
        assertNull(out.get("optional_label"));
        assertEquals(input.size(), out.size());
    }

    public void testEncryptedCarrierIsDecryptedToPlaintextString() {
        String canary = "AKIA_canary_" + randomAlphaOfLength(8);
        EncryptedData encrypted = IDENTITY.encrypt(canary.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> input = new HashMap<>();
        input.put("region", "us-east-1");
        input.put("secret_access_key", encrypted);

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertThat(out.get("region"), equalTo("us-east-1"));
        assertThat(out.get("secret_access_key"), instanceOf(String.class));
        assertThat(out.get("secret_access_key"), equalTo(canary));
    }

    public void testMultipleEncryptedCarriersAreEachDecrypted() {
        String akCanary = "ak_" + randomAlphaOfLength(8);
        String skCanary = "sk_" + randomAlphaOfLength(12);
        Map<String, Object> input = new HashMap<>();
        input.put("access_key", IDENTITY.encrypt(akCanary.getBytes(StandardCharsets.UTF_8)));
        input.put("secret_key", IDENTITY.encrypt(skCanary.getBytes(StandardCharsets.UTF_8)));
        input.put("endpoint", "https://example.test");

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertEquals(akCanary, out.get("access_key"));
        assertEquals(skCanary, out.get("secret_key"));
        assertEquals("https://example.test", out.get("endpoint"));
    }

    public void testUnknownObjectsPassThroughUntouched() {
        // Anything that isn't an EncryptedData stays as-is. The decrypt seam doesn't reject foreign
        // shapes — it only decrypts what it knows. The corruption-check policy (a String for a
        // secret-flagged setting is stale pre-encryption data) is enforced upstream at PUT time.
        Map<String, Object> input = new HashMap<>();
        input.put("nested", Map.of("k", "v"));
        input.put("list", java.util.List.of(1, 2, 3));

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertEquals(input.get("nested"), out.get("nested"));
        assertEquals(input.get("list"), out.get("list"));
    }

    public void testDecryptInPlaceProducesACopyNotMutatingTheInput() {
        EncryptedData encrypted = IDENTITY.encrypt("canary".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> input = new HashMap<>();
        input.put("secret_access_key", encrypted);
        Map<String, Object> snapshot = new HashMap<>(input);

        DataSourceCredentials.decryptInPlace(input);

        assertEquals(snapshot, input);
        assertSame(encrypted, input.get("secret_access_key"));
    }

    public void testUnboundEncryptionServiceFailsLoudWhenAnyEncryptedCarrierPresent() throws Exception {
        // Symmetric with the PUT-side "encrypt or fail" rule: if the connector boundary is reached
        // without an EncryptionService binding but the settings carry encrypted material, the read
        // must fail with 503 rather than silently passing an EncryptedData to the SDK as opaque junk.
        DataSourceCredentials.initialize(null);
        try {
            EncryptedData encrypted = IDENTITY.encrypt("plain".getBytes(StandardCharsets.UTF_8));
            Map<String, Object> input = new HashMap<>();
            input.put("region", "us-east-1");
            input.put("secret_access_key", encrypted);

            ElasticsearchStatusException ese = expectThrows(
                ElasticsearchStatusException.class,
                () -> DataSourceCredentials.decryptInPlace(input)
            );
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, ese.status());
            assertThat(ese.getMessage(), containsString("encryption service is not bound"));
        } finally {
            DataSourceCredentials.initialize(IDENTITY);
        }
    }

    public void testUnboundServiceWithNoEncryptedValuesIsAllowedThrough() throws Exception {
        // Plaintext-only settings reach the connector even when no service is bound — there is nothing
        // to decrypt. The 503 fires only when an EncryptedData carrier is actually present.
        DataSourceCredentials.initialize(null);
        try {
            Map<String, Object> input = new HashMap<>();
            input.put("region", "us-east-1");
            input.put("endpoint", "https://example.test");
            Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);
            assertEquals("us-east-1", out.get("region"));
            assertEquals("https://example.test", out.get("endpoint"));
        } finally {
            DataSourceCredentials.initialize(IDENTITY);
        }
    }
}
