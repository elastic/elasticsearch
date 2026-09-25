/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest.GrantType;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

public class CreateTokenRequestTests extends ESTestCase {

    public void testRequestValidation() {
        CreateTokenRequest request = new CreateTokenRequest();
        ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(
            ve.validationErrors().get(0),
            containsString("[password, _kerberos, refresh_token, client_credentials, _user_managed_service_account]")
        );
        assertThat(ve.validationErrors().get(0), containsString("grant_type"));

        request.setGrantType("password");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("username is missing"));
        assertThat(ve.validationErrors(), hasItem("password is missing"));

        request.setUsername(randomBoolean() ? null : "");
        request.setPassword(randomBoolean() ? null : new SecureString(new char[] {}));

        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("username is missing"));
        assertThat(ve.validationErrors(), hasItem("password is missing"));

        request.setUsername(randomAlphaOfLengthBetween(1, 256));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("password is missing"));

        request.setPassword(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        ve = request.validate();
        assertNull(ve);

        request.setRefreshToken(randomAlphaOfLengthBetween(1, 10));
        request.setKerberosTicket(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));

        request.setGrantType("refresh_token");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(3, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));

        request.setUsername(null);
        request.setPassword(null);
        request.setKerberosTicket(null);
        ve = request.validate();
        assertNull(ve);

        request.setRefreshToken(null);
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("refresh_token is missing"));

        request.setGrantType("client_credentials");
        ve = request.validate();
        assertNull(ve);

        request.setUsername(randomAlphaOfLengthBetween(1, 32));
        request.setPassword(new SecureString(randomAlphaOfLengthBetween(1, 32).toCharArray()));
        request.setKerberosTicket(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        request.setRefreshToken(randomAlphaOfLengthBetween(1, 32));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(4, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));

        request.setGrantType("_kerberos");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(3, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));

        request.setUsername(null);
        request.setPassword(null);
        request.setRefreshToken(null);
        ve = request.validate();
        assertNull(ve);

        request.setGrantType("_user_managed_service_account");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));
        assertThat(ve.validationErrors(), hasItem("service_account_token is missing"));

        request.setKerberosTicket(null);
        request.setServiceAccountToken(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        ve = request.validate();
        assertNull(ve);

        request.setUsername(randomAlphaOfLengthBetween(1, 32));
        request.setPassword(new SecureString(randomAlphaOfLengthBetween(1, 32).toCharArray()));
        request.setKerberosTicket(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        request.setRefreshToken(randomAlphaOfLengthBetween(1, 32));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(4, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));

        request.setGrantType("client_credentials");
        request.setUsername(null);
        request.setPassword(null);
        request.setKerberosTicket(null);
        request.setRefreshToken(null);
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("service_account_token is not supported")));
    }

    public void testSerialization() throws IOException {
        final String grantType = randomFrom(Arrays.stream(GrantType.values()).map(gt -> gt.getValue()).collect(Collectors.toList()));
        final String username = randomBoolean() ? randomAlphaOfLength(5) : null;
        final String scope = randomBoolean() ? randomAlphaOfLength(5) : null;
        final SecureString password = randomBoolean() ? new SecureString(new char[] { 'p', 'a', 's', 's' }) : null;
        final SecureString kerberosTicket = randomBoolean() ? new SecureString(new char[] { 'k', 'e', 'r', 'b' }) : null;
        final String refreshToken = randomBoolean() ? randomAlphaOfLength(5) : null;
        final SecureString serviceAccountToken = randomBoolean() ? new SecureString(new char[] { 'u', 'm', 's', 'a' }) : null;
        final CreateTokenRequest request = new CreateTokenRequest(
            grantType,
            username,
            password,
            kerberosTicket,
            scope,
            refreshToken,
            serviceAccountToken
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final CreateTokenRequest serialized = new CreateTokenRequest(in);
                assertEquals(grantType, serialized.getGrantType());
                if (scope != null) {
                    assertEquals(scope, serialized.getScope());
                }
                if (password != null) {
                    assertEquals(password, serialized.getPassword());
                }
                if (kerberosTicket != null) {
                    assertEquals(kerberosTicket, serialized.getKerberosTicket());
                }
                if (refreshToken != null) {
                    assertEquals(refreshToken, serialized.getRefreshToken());
                }
                if (serviceAccountToken != null) {
                    assertEquals(serviceAccountToken, serialized.getServiceAccountToken());
                }
            }
        }
    }

    public void testSerializationToOldVersionThrowsWhenServiceAccountTokenIsSet() throws IOException {
        final TransportVersion umsaTokenExchangeVersion = TransportVersion.fromName("umsa_oauth2_token_exchange");
        final CreateTokenRequest request = new CreateTokenRequest(
            GrantType.USER_MANAGED_SERVICE_ACCOUNT.getValue(),
            null,
            null,
            null,
            null,
            null,
            new SecureString("umsa-token".toCharArray())
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.randomVersionNotSupporting(umsaTokenExchangeVersion));
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.writeTo(out));
            assertThat(
                e.getMessage(),
                containsString(
                    "versions of Elasticsearch before ["
                        + umsaTokenExchangeVersion.toReleaseVersion()
                        + "] can't handle the [_user_managed_service_account] grant type and attempted to send to ["
                        + out.getTransportVersion().toReleaseVersion()
                        + "]"
                )
            );
        }
    }

    public void testSerializationToOldVersionWorksWithoutServiceAccountToken() throws IOException {
        final TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(
            TransportVersion.fromName("umsa_oauth2_token_exchange")
        );
        final CreateTokenRequest request = new CreateTokenRequest(
            GrantType.PASSWORD.getValue(),
            "user",
            new SecureString("password".toCharArray()),
            null,
            null,
            null
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(oldVersion);
                final CreateTokenRequest serialized = new CreateTokenRequest(in);
                assertEquals(request.getGrantType(), serialized.getGrantType());
                assertEquals(request.getUsername(), serialized.getUsername());
                assertEquals(request.getPassword(), serialized.getPassword());
                assertNull(serialized.getServiceAccountToken());
            }
        }
    }
}
