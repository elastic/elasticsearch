/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.namedcredentials;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.namedcredentials.CredentialAuthType;
import org.elasticsearch.xpack.core.security.action.namedcredentials.NamedCredential;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class NamedCredentialsServiceTests extends ESTestCase {

    /**
     * Fake that round-trips payloads without real crypto. The real AesGcmEncryptionService needs
     * cluster-state PEK plumbing that only exists on a running node; the encrypt/decrypt contract
     * (EncryptedData in, same bytes out) is all the service under test relies on.
     */
    private static final EncryptionService FAKE_ENCRYPTION = new EncryptionService() {
        @Override
        public EncryptedData encrypt(byte[] bytes) {
            return new EncryptedData("test-key", bytes);
        }

        @Override
        public byte[] decrypt(EncryptedData encryptedData) {
            return encryptedData.payload();
        }
    };

    public void testEncryptDecryptAuthRoundTrip() {
        Map<String, String> auth = Map.of("username", "u", "password", "hunter2");
        String blob = NamedCredentialsService.encryptAuth(FAKE_ENCRYPTION, auth);
        assertThat(blob, not(containsString("hunter2"))); // base64 of stream-serialized carrier
        assertThat(NamedCredentialsService.decryptAuth(FAKE_ENCRYPTION, blob), equalTo(auth));
    }

    public void testBuildSourceAndParseBack() {
        Map<String, String> auth = Map.of("client_id", "a", "client_secret", "b");
        Map<String, String> config = Map.of("token_url", "https://example.com/token");
        String blob = NamedCredentialsService.encryptAuth(FAKE_ENCRYPTION, auth);
        Map<String, Object> source = NamedCredentialsService.buildSource(
            "oauth_client_credentials",
            "https://example.com",
            config,
            blob,
            1000L,
            2000L
        );
        assertThat(source.get("auth_type"), equalTo("oauth_client_credentials"));
        assertThat(source.get("auth"), equalTo(blob));
        assertThat(source.get("created_at"), equalTo(1000L));
        assertThat(source.get("updated_at"), equalTo(2000L));

        NamedCredential parsed = NamedCredentialsService.credentialFromSource("c1", source);
        assertThat(parsed.name(), equalTo("c1"));
        assertThat(parsed.authType(), is(CredentialAuthType.OAUTH_CLIENT_CREDENTIALS));
        assertThat(parsed.url(), equalTo("https://example.com"));
        assertThat(parsed.config(), hasEntry("token_url", "https://example.com/token"));
        assertThat(parsed.auth(), nullValue());
        assertThat(parsed.createdAtMillis(), equalTo(1000L));
    }

    public void testBuildSourceOmitsNullUrl() {
        Map<String, Object> source = NamedCredentialsService.buildSource("bearer", null, Map.of(), "blob", 1L, 1L);
        assertThat(source.containsKey("url"), is(false));
        NamedCredential parsed = NamedCredentialsService.credentialFromSource("c1", source);
        assertThat(parsed.url(), nullValue());
    }
}
