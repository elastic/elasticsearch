/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.io.IOException;
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

    /** Encrypt the given plaintext and wrap it as the {@link EncryptedSecret} the connector map carries. */
    private static EncryptedSecret encryptedSecret(String plaintext) throws IOException {
        EncryptedData encrypted = IDENTITY.encrypt(plaintext.getBytes(StandardCharsets.UTF_8));
        BytesStreamOutput out = new BytesStreamOutput();
        encrypted.writeTo(out);
        return new EncryptedSecret(BytesReference.toBytes(out.bytes()));
    }

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

    public void testEncryptedBlobIsDecryptedToPlaintextString() throws IOException {
        String canary = "AKIA_canary_" + randomAlphaOfLength(8);
        EncryptedSecret blob = encryptedSecret(canary);

        Map<String, Object> input = new HashMap<>();
        input.put("region", "us-east-1");
        input.put("secret_access_key", blob);

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertThat(out.get("region"), equalTo("us-east-1"));
        assertThat(out.get("secret_access_key"), instanceOf(String.class));
        assertThat(out.get("secret_access_key"), equalTo(canary));
    }

    public void testMultipleEncryptedBlobsAreEachDecrypted() throws IOException {
        String akCanary = "ak_" + randomAlphaOfLength(8);
        String skCanary = "sk_" + randomAlphaOfLength(12);
        Map<String, Object> input = new HashMap<>();
        input.put("access_key", encryptedSecret(akCanary));
        input.put("secret_key", encryptedSecret(skCanary));
        input.put("endpoint", "https://example.test");

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertEquals(akCanary, out.get("access_key"));
        assertEquals(skCanary, out.get("secret_key"));
        assertEquals("https://example.test", out.get("endpoint"));
    }

    public void testPlaintextSecretsPassThrough() throws IOException {
        // Plaintext String values for secrets (the no-encryption-service path on the producer side)
        // reach the connector untouched — the SDK can use the plaintext String directly.
        Map<String, Object> input = new HashMap<>();
        input.put("region", "us-east-1");
        input.put("secret_access_key", "AKIA_plaintext_storage");

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertEquals("us-east-1", out.get("region"));
        assertEquals("AKIA_plaintext_storage", out.get("secret_access_key"));
    }

    public void testRawByteArrayIsNotTreatedAsEncrypted() {
        // The discriminator is the EncryptedSecret type, not "is it bytes". A bare byte[] in the
        // config map is a legitimate plaintext binary value and must pass through untouched.
        byte[] raw = new byte[] { 1, 2, 3, 4 };
        Map<String, Object> input = new HashMap<>();
        input.put("some_binary_value", raw);

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertSame(raw, out.get("some_binary_value"));
    }

    public void testUnknownObjectsPassThroughUntouched() {
        Map<String, Object> input = new HashMap<>();
        input.put("nested", Map.of("k", "v"));
        input.put("list", java.util.List.of(1, 2, 3));

        Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);

        assertEquals(input.get("nested"), out.get("nested"));
        assertEquals(input.get("list"), out.get("list"));
    }

    public void testDecryptInPlaceProducesACopyNotMutatingTheInput() throws IOException {
        EncryptedSecret blob = encryptedSecret("canary");
        Map<String, Object> input = new HashMap<>();
        input.put("secret_access_key", blob);
        Map<String, Object> snapshot = new HashMap<>(input);

        DataSourceCredentials.decryptInPlace(input);

        assertEquals(snapshot, input);
        assertSame(blob, input.get("secret_access_key"));
    }

    public void testUnboundEncryptionServiceFailsLoudWhenEncryptedBlobPresent() throws Exception {
        // Consumer-side strict: if an encrypted blob reaches the connector boundary without an
        // EncryptionService to decrypt it, fail with 503 rather than hand the SDK opaque bytes.
        DataSourceCredentials.initialize(null);
        try {
            EncryptedSecret blob = encryptedSecret("plain");
            Map<String, Object> input = new HashMap<>();
            input.put("region", "us-east-1");
            input.put("secret_access_key", blob);

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

    public void testUnboundServiceWithPlaintextSecretsIsAllowedThrough() throws Exception {
        // The no-encryption-service producer path produces plaintext String values; reading them back
        // on a node still without a service is fine — there is nothing to decrypt.
        DataSourceCredentials.initialize(null);
        try {
            Map<String, Object> input = new HashMap<>();
            input.put("region", "us-east-1");
            input.put("secret_access_key", "AKIA_plaintext");
            Map<String, Object> out = DataSourceCredentials.decryptInPlace(input);
            assertEquals("us-east-1", out.get("region"));
            assertEquals("AKIA_plaintext", out.get("secret_access_key"));
        } finally {
            DataSourceCredentials.initialize(IDENTITY);
        }
    }
}
