/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CreateTokenRequestTests extends ESTestCase {

    public void testCreateTokenFromPassword() {
        final CreateTokenRequest request = CreateTokenRequest.passwordGrant("jsmith", "top secret password".toCharArray());
        assertThat(request.getGrantType(), equalTo("password"));
        assertThat(request.getUsername(), equalTo("jsmith"));
        assertThat(new String(request.getPassword()), equalTo("top secret password"));
        assertThat(request.getScope(), nullValue());
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(
            Strings.toString(request),
            equalTo("{\"grant_type\":\"password\",\"username\":\"jsmith\",\"password\":\"top secret password\"}")
        );
    }

    public void testCreateTokenFromRefreshToken() {
        final CreateTokenRequest request = CreateTokenRequest.refreshTokenGrant("9a7f41cf-9918-4d1f-bfaa-ad3f8f9f02b9");
        assertThat(request.getGrantType(), equalTo("refresh_token"));
        assertThat(request.getRefreshToken(), equalTo("9a7f41cf-9918-4d1f-bfaa-ad3f8f9f02b9"));
        assertThat(request.getScope(), nullValue());
        assertThat(request.getUsername(), nullValue());
        assertThat(request.getPassword(), nullValue());
        assertThat(
            Strings.toString(request),
            equalTo("{\"grant_type\":\"refresh_token\",\"refresh_token\":\"9a7f41cf-9918-4d1f-bfaa-ad3f8f9f02b9\"}")
        );
    }

    public void testCreateTokenFromClientCredentials() {
        final CreateTokenRequest request = CreateTokenRequest.clientCredentialsGrant();
        assertThat(request.getGrantType(), equalTo("client_credentials"));
        assertThat(request.getScope(), nullValue());
        assertThat(request.getUsername(), nullValue());
        assertThat(request.getPassword(), nullValue());
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(Strings.toString(request), equalTo("{\"grant_type\":\"client_credentials\"}"));
    }

    public void testCreateTokenFromKerberosTicket() {
        final CreateTokenRequest request = CreateTokenRequest.kerberosGrant("top secret kerberos ticket".toCharArray());
        assertThat(request.getGrantType(), equalTo("_kerberos"));
        assertThat(request.getScope(), nullValue());
        assertThat(request.getUsername(), nullValue());
        assertThat(request.getPassword(), nullValue());
        assertThat(request.getRefreshToken(), nullValue());
        assertThat(new String(request.getKerberosTicket()), equalTo("top secret kerberos ticket"));
        assertThat(
            Strings.toString(request),
            equalTo("{\"grant_type\":\"_kerberos\",\"kerberos_ticket\":\"top secret kerberos ticket\"}")
        );
    }

    public void testEqualsAndHashCode() {
        final String grantType = randomAlphaOfLength(8);
        final String scope = randomBoolean() ? null : randomAlphaOfLength(6);
        final String username = randomBoolean() ? null : randomAlphaOfLengthBetween(4, 10);
        final char[] password = randomBoolean() ? null : randomAlphaOfLengthBetween(8, 12).toCharArray();
        final String refreshToken = randomBoolean() ? null : randomAlphaOfLengthBetween(12, 24);
        final char[] kerberosTicket = randomBoolean() ? null : randomAlphaOfLengthBetween(8, 12).toCharArray();
        final CreateTokenRequest request = new CreateTokenRequest(grantType, scope, username, password, refreshToken, kerberosTicket);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
            r -> new CreateTokenRequest(r.getGrantType(), r.getScope(), r.getUsername(), r.getPassword(),
                                        r.getRefreshToken(), r.getKerberosTicket()),
            this::mutate);
    }

    private CreateTokenRequest mutate(CreateTokenRequest req) {
        switch (randomIntBetween(1, 6)) {
        case 1:
            return new CreateTokenRequest("g", req.getScope(), req.getUsername(), req.getPassword(), req.getRefreshToken(),
                    req.getKerberosTicket());
        case 2:
            return new CreateTokenRequest(req.getGrantType(), "s", req.getUsername(), req.getPassword(), req.getRefreshToken(),
                    req.getKerberosTicket());
        case 3:
            return new CreateTokenRequest(req.getGrantType(), req.getScope(), "u", req.getPassword(), req.getRefreshToken(),
                    req.getKerberosTicket());
        case 4:
            final char[] password = { 'p' };
            return new CreateTokenRequest(req.getGrantType(), req.getScope(), req.getUsername(), password, req.getRefreshToken(),
                    req.getKerberosTicket());
        case 5:
            final char[] kerberosTicket = { 'k' };
            return new CreateTokenRequest(req.getGrantType(), req.getScope(), req.getUsername(), req.getPassword(), req.getRefreshToken(),
                    kerberosTicket);
        case 6:
            return new CreateTokenRequest(req.getGrantType(), req.getScope(), req.getUsername(), req.getPassword(), "r",
                    req.getKerberosTicket());
        }
        throw new IllegalStateException("Bad random number");
    }

}
