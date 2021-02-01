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

import org.elasticsearch.Version;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DelegatePkiAuthenticationResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse,
        DelegatePkiAuthenticationResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse createServerTestInstance(
        XContentType xContentType) {
        return new org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse(randomAlphaOfLength(6),
                TimeValue.parseTimeValue(randomTimeValue(), getClass().getSimpleName() + ".expiresIn"),
                createAuthentication());
    }

    @Override
    protected DelegatePkiAuthenticationResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return DelegatePkiAuthenticationResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse serverTestInstance,
            DelegatePkiAuthenticationResponse clientInstance) {
        assertThat(serverTestInstance.getAccessToken(), is(clientInstance.getAccessToken()));
        assertThat(serverTestInstance.getExpiresIn(), is(clientInstance.getExpiresIn()));
        assertThat(clientInstance.getType(), is("Bearer"));
        AuthenticateResponse serverAuthenticationResponse = createServerAuthenticationResponse(serverTestInstance.getAuthentication());
        User user = serverTestInstance.getAuthentication().getUser();
        assertThat(serverAuthenticationResponse, equalTo(clientInstance.getAuthentication()));
    }

    protected Authentication createAuthentication() {
        final String username = randomAlphaOfLengthBetween(1, 4);
        final String[] roles = generateRandomStringArray(4, 4, false, true);
        final Map<String, Object> metadata;
        metadata = new HashMap<>();
        if (randomBoolean()) {
            metadata.put("string", null);
        } else {
            metadata.put("string", randomAlphaOfLengthBetween(0, 4));
        }
        if (randomBoolean()) {
            metadata.put("string_list", null);
        } else {
            metadata.put("string_list", Arrays.asList(generateRandomStringArray(4, 4, false, true)));
        }
        final String fullName = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 4));
        final String email = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 4));
        final boolean enabled = randomBoolean();
        final String authenticationRealmName = randomAlphaOfLength(5);
        final String authenticationRealmType = randomFrom("file", "native", "ldap", "active_directory", "saml", "kerberos");
        final String lookupRealmName = randomAlphaOfLength(5);
        final String lookupRealmType = randomFrom("file", "native", "ldap", "active_directory", "saml", "kerberos");
        final String nodeName = randomAlphaOfLengthBetween(1, 10);
        final Authentication.AuthenticationType authenticationType = randomFrom(Authentication.AuthenticationType.values());
        return new Authentication(
            new User(username, roles, fullName, email, metadata, true),
            new Authentication.RealmRef(authenticationRealmName, authenticationRealmType, nodeName),
            new Authentication.RealmRef(lookupRealmName, lookupRealmType, nodeName), Version.CURRENT, authenticationType, metadata);
    }

    AuthenticateResponse createServerAuthenticationResponse(Authentication authentication){
        User user = authentication.getUser();
        org.elasticsearch.client.security.user.User cUser = new org.elasticsearch.client.security.user.User(user.principal(),
            Arrays.asList(user.roles()), user.metadata(), user.fullName(), user.email());
        AuthenticateResponse.RealmInfo authenticatedBy = new AuthenticateResponse.RealmInfo(authentication.getAuthenticatedBy().getName(),
            authentication.getAuthenticatedBy().getType());
        AuthenticateResponse.RealmInfo lookedUpBy = new AuthenticateResponse.RealmInfo(authentication.getLookedUpBy() == null?
            authentication.getAuthenticatedBy().getName(): authentication.getLookedUpBy().getName(),
            authentication.getLookedUpBy() == null?
                authentication.getAuthenticatedBy().getType(): authentication.getLookedUpBy().getType());
        return new AuthenticateResponse(cUser, user.enabled(), authenticatedBy, lookedUpBy,
            authentication.getAuthenticationType().toString().toLowerCase(Locale.ROOT));
    }
}
