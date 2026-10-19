/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Build the {@link EncryptedData} carrier the connector map holds for a secret. The payload mirrors
     * the producer side: the value is {@code writeGenericValue}-serialized before encryption, so the
     * consumer's {@code readGenericValue} restores the original type.
     */
    private static EncryptedData encryptedSecret(Object value) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(value);
            return IDENTITY.encrypt(BytesReference.toBytes(out.bytes()));
        }
    }

    private DataSourceCredentials credentials;

    @Before
    public void initCredentials() {
        credentials = new DataSourceCredentials(IDENTITY);
    }

    public void testNullMapPassesThrough() {
        assertNull(credentials.decryptInPlace(null));
    }

    public void testEmptyMapReturnsCopy() {
        Map<String, Object> empty = Map.of();
        Map<String, Object> out = credentials.decryptInPlace(empty);
        assertNotNull(out);
        assertTrue(out.isEmpty());
    }

    public void testNonSecretsPreserved() {
        Map<String, Object> input = new HashMap<>();
        input.put("region", "us-east-1");
        input.put("max_retries", 7);
        input.put("use_path_style", true);
        input.put("optional_label", null);

        Map<String, Object> out = credentials.decryptInPlace(input);

        assertEquals("us-east-1", out.get("region"));
        assertEquals(7, out.get("max_retries"));
        assertEquals(true, out.get("use_path_style"));
        assertNull(out.get("optional_label"));
        assertEquals(input.size(), out.size());
    }

    public void testEncryptedBlobIsDecryptedToPlaintextString() throws IOException {
        String canary = "AKIA_canary_" + randomAlphaOfLength(8);
        EncryptedData blob = encryptedSecret(canary);

        Map<String, Object> input = new HashMap<>();
        input.put("region", "us-east-1");
        input.put("secret_access_key", blob);

        Map<String, Object> out = credentials.decryptInPlace(input);

        assertNotSame("decryptInPlace must return a fresh map, never alias its input", input, out);
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

        Map<String, Object> out = credentials.decryptInPlace(input);

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

        Map<String, Object> out = credentials.decryptInPlace(input);

        assertEquals("us-east-1", out.get("region"));
        assertEquals("AKIA_plaintext_storage", out.get("secret_access_key"));
    }

    public void testRawByteArrayIsNotTreatedAsEncrypted() {
        // The discriminator is the EncryptedData type, not "is it bytes". A bare byte[] in the
        // config map is a legitimate plaintext binary value and must pass through untouched.
        byte[] raw = new byte[] { 1, 2, 3, 4 };
        Map<String, Object> input = new HashMap<>();
        input.put("some_binary_value", raw);

        Map<String, Object> out = credentials.decryptInPlace(input);

        assertSame(raw, out.get("some_binary_value"));
    }

    public void testUnknownObjectsPassThroughUntouched() {
        Map<String, Object> input = new HashMap<>();
        input.put("nested", Map.of("k", "v"));
        input.put("list", java.util.List.of(1, 2, 3));

        Map<String, Object> out = credentials.decryptInPlace(input);

        assertEquals(input.get("nested"), out.get("nested"));
        assertEquals(input.get("list"), out.get("list"));
    }

    public void testDecryptInPlaceProducesACopyNotMutatingTheInput() throws IOException {
        EncryptedData blob = encryptedSecret("canary");
        Map<String, Object> input = new HashMap<>();
        input.put("secret_access_key", blob);
        Map<String, Object> snapshot = new HashMap<>(input);

        credentials.decryptInPlace(input);

        assertEquals(snapshot, input);
        assertSame(blob, input.get("secret_access_key"));
    }

    /** Decrypt stub that reports the key as not yet available on every call, counting the calls. */
    private static EncryptionService keyNotYetAvailable(AtomicInteger attempts) {
        return new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData("test-key", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                attempts.incrementAndGet();
                throw new EncryptionKeyNotYetAvailableException("project encryption key [{}] is not yet available", encryptedData.keyId());
            }
        };
    }

    /** Identity decrypt that counts calls. */
    private static EncryptionService counting(AtomicInteger attempts) {
        return new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData("test-key", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                attempts.incrementAndGet();
                return encryptedData.payload();
            }
        };
    }

    private static ExternalSourceExec externalSource(Map<String, Object> config) {
        return new ExternalSourceExec(Source.EMPTY, "teststore:///data/*.parquet", "parquet", List.of(), config, Map.of(), null, null);
    }

    public void testKeyNotYetAvailableBecomesCredentialsNotReady() throws IOException {
        DataSourceCredentials credentials = new DataSourceCredentials(keyNotYetAvailable(new AtomicInteger()));
        EncryptedData secret = encryptedSecret("s3cr3t");

        ExternalUnavailableException e = expectThrows(
            ExternalUnavailableException.class,
            () -> credentials.decryptInPlace(Map.of("secret", secret))
        );

        assertThat(e.condition(), equalTo(ExternalException.Condition.CREDENTIALS_NOT_READY));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(e.getCause(), instanceOf(EncryptionKeyNotYetAvailableException.class));
        assertThat(e.getMessage(), containsString("cannot decrypt the external data source credentials yet"));
        assertThat(e.getMessage(), containsString("key [test-key]"));
        assertTrue(ExternalException.hasCondition(e, ExternalException.Condition.CREDENTIALS_NOT_READY));
    }

    public void testCredentialsNotReadySurvivesTheWire() throws IOException {
        DataSourceCredentials credentials = new DataSourceCredentials(keyNotYetAvailable(new AtomicInteger()));
        EncryptedData secret = encryptedSecret("s3cr3t");
        Exception local = expectThrows(ExternalUnavailableException.class, () -> credentials.decryptInPlace(Map.of("secret", secret)));

        Exception remote;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(local);
            try (StreamInput in = out.bytes().streamInput()) {
                remote = in.readException();
            }
        }

        // The type is not in the exception registry, so it crosses a node boundary as a wrapper. The condition rides in the
        // metadata and the status is kept, which is all the coordinator needs to route the splits elsewhere.
        assertThat(remote, instanceOf(NotSerializableExceptionWrapper.class));
        assertThat(ExternalException.conditionOf(remote), equalTo(ExternalException.Condition.CREDENTIALS_NOT_READY));
        assertThat(ExceptionsHelper.status(remote), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testPermanentUnavailabilityIsNotTranslated() throws IOException {
        EncryptionService disabled = new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData("test-key", bytes.clone());
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                throw new EncryptionServiceUnavailableException(EncryptionServiceState.DISABLED);
            }
        };
        DataSourceCredentials credentials = new DataSourceCredentials(disabled);
        EncryptedData secret = encryptedSecret("s3cr3t");

        // another node cannot help with a permanent condition, so it keeps its own type
        expectThrows(EncryptionServiceUnavailableException.class, () -> credentials.decryptInPlace(Map.of("secret", secret)));
    }

    public void testPreflightDecryptsEverySecretUnderTheDataSourceMap() throws IOException {
        AtomicInteger attempts = new AtomicInteger();
        DataSourceCredentials credentials = new DataSourceCredentials(counting(attempts));
        Map<String, Object> config = Map.of(
            "format",
            "parquet",
            ExternalSourceResolver.DATASOURCE_CONFIG_KEY,
            Map.of("secret_token", encryptedSecret("t"), "secret_key", encryptedSecret("k"), "region", "us-east-1")
        );

        credentials.preflight(externalSource(config));

        assertThat("both secrets under _datasource are decrypted, the plain setting is not", attempts.get(), equalTo(2));
    }

    public void testPreflightFailsFastWhenTheKeyIsNotThere() throws IOException {
        DataSourceCredentials credentials = new DataSourceCredentials(keyNotYetAvailable(new AtomicInteger()));
        Map<String, Object> config = Map.of(ExternalSourceResolver.DATASOURCE_CONFIG_KEY, Map.of("secret_token", encryptedSecret("t")));

        ExternalUnavailableException e = expectThrows(
            ExternalUnavailableException.class,
            () -> credentials.preflight(externalSource(config))
        );

        assertThat(e.condition(), equalTo(ExternalException.Condition.CREDENTIALS_NOT_READY));
    }

    public void testPreflightWithoutSecretsDoesNotDecrypt() {
        AtomicInteger attempts = new AtomicInteger();
        DataSourceCredentials credentials = new DataSourceCredentials(counting(attempts));

        credentials.preflight(externalSource(Map.of("format", "parquet")));

        assertThat(attempts.get(), equalTo(0));
    }
}
