/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class JwtUtilTests extends JwtTestCase {

    public void testClientAuthenticationTypeValidation() {
        final String clientAuthenticationTypeKey = JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE.getKey();
        final String clientAuthenticationSharedSecretKey = JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET.getKey();
        final SecureString sharedSecretNonEmpty = new SecureString(randomAlphaOfLengthBetween(1, 32).toCharArray());
        final SecureString sharedSecretNullOrEmpty = randomBoolean() ? new SecureString("".toCharArray()) : null;

        // If type is None, verify null or empty is accepted
        JwtUtil.validateClientAuthenticationSettings(
            clientAuthenticationTypeKey,
            JwtRealmSettings.ClientAuthenticationType.NONE,
            clientAuthenticationSharedSecretKey,
            sharedSecretNullOrEmpty
        );
        // If type is None, verify non-empty is rejected
        final Exception exception1 = expectThrows(
            SettingsException.class,
            () -> JwtUtil.validateClientAuthenticationSettings(
                clientAuthenticationTypeKey,
                JwtRealmSettings.ClientAuthenticationType.NONE,
                clientAuthenticationSharedSecretKey,
                sharedSecretNonEmpty
            )
        );
        assertThat(
            exception1.getMessage(),
            is(
                equalTo(
                    "Setting ["
                        + clientAuthenticationSharedSecretKey
                        + "] is not supported, because setting ["
                        + clientAuthenticationTypeKey
                        + "] is ["
                        + JwtRealmSettings.ClientAuthenticationType.NONE.value()
                        + "]"
                )
            )
        );

        // If type is SharedSecret, verify non-empty is accepted
        JwtUtil.validateClientAuthenticationSettings(
            clientAuthenticationTypeKey,
            JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET,
            clientAuthenticationSharedSecretKey,
            sharedSecretNonEmpty
        );
        // If type is SharedSecret, verify null or empty is rejected
        final Exception exception2 = expectThrows(
            SettingsException.class,
            () -> JwtUtil.validateClientAuthenticationSettings(
                clientAuthenticationTypeKey,
                JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET,
                clientAuthenticationSharedSecretKey,
                sharedSecretNullOrEmpty
            )
        );
        assertThat(
            exception2.getMessage(),
            is(
                equalTo(
                    "Missing setting for ["
                        + clientAuthenticationSharedSecretKey
                        + "]. It is required when setting ["
                        + clientAuthenticationTypeKey
                        + "] is ["
                        + JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET.value()
                        + "]"
                )
            )
        );
    }

    public void testParseHttpsUriAllowsFilesPassThrough() {
        // Invalid null or empty values should be rejected
        assertThat(JwtUtil.parseHttpsUri(null), is(nullValue()));
        assertThat(JwtUtil.parseHttpsUri(""), is(nullValue()));
        // Valid Windows local file paths should be rejected
        assertThat(JwtUtil.parseHttpsUri("C:"), is(nullValue()));
        assertThat(JwtUtil.parseHttpsUri("C:/"), is(nullValue()));
        assertThat(JwtUtil.parseHttpsUri("C:/jwkset.json"), is(nullValue()));
        // Valid Linux local file paths should be rejected
        assertThat(JwtUtil.parseHttpsUri("/"), is(nullValue()));
        assertThat(JwtUtil.parseHttpsUri("/tmp"), is(nullValue()));
        assertThat(JwtUtil.parseHttpsUri("/tmp/"), is(nullValue()));
        assertThat(JwtUtil.parseHttpsUri("/tmp/jwkset.json"), is(nullValue()));
    }

    public void testParseHttpUriAllRejected() {
        // Reject HTTP URIs
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http:"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http:/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://:80"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://:8080/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:80"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:80/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:8080"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:8080/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com/jwkset.json"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:80/jwkset.json"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:8080/jwkset.json"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com/path/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:80/path/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:8080/path/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com/path/jwkset.json"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:80/path/jwkset.json"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("http://example.com:8080/path/jwkset.json"));
    }

    public void testParseHttpsUriProblemsRejected() {
        // Reject invalid HTTPS URIs where parse failed
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("https"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("https:"));
        // Reject valid HTTPS URIs where host is missing
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("https:/"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("https://"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("https://:443"));
        expectThrows(SettingsException.class, () -> JwtUtil.parseHttpsUri("https://:443/"));
    }

    public void testParseHttpsUriAccepted() {
        // Accept HTTPS URIs if parse succeeds
        assertThat(JwtUtil.parseHttpsUri("https://example.com"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com/"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:443"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:443/"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:8443"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:8443/"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com/path/"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:443/path/"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:8443/path/"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com/jwkset.json"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:443/jwkset.json"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:8443/jwkset.json"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com/path/jwkset.json"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:443/path/jwkset.json"), is(notNullValue()));
        assertThat(JwtUtil.parseHttpsUri("https://example.com:8443/path/jwkset.json"), is(notNullValue()));
    }

    public void testParseHttpsUri() {}
}
