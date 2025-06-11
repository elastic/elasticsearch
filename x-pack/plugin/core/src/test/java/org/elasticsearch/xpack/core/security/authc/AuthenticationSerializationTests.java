/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationSerializationHelper;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class AuthenticationSerializationTests extends ESTestCase {

    public void testWriteToAndReadFrom() throws Exception {
        User user = new User(randomAlphaOfLengthBetween(4, 30), generateRandomStringArray(20, 30, false));
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(user, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, not(sameInstance(user)));
        assertThat(readFrom.principal(), is(user.principal()));
        assertThat(Arrays.equals(readFrom.roles(), user.roles()), is(true));
    }

    public void testWriteToAndReadFromWithRunAs() throws Exception {
        final Authentication authentication = AuthenticationTestHelper.builder().runAs().build();
        assertThat(authentication.isRunAs(), is(true));

        BytesStreamOutput output = new BytesStreamOutput();
        authentication.writeTo(output);
        final Authentication readFrom = new Authentication(output.bytes().streamInput());
        assertThat(readFrom.isRunAs(), is(true));

        assertThat(readFrom, not(sameInstance(authentication)));
        final User readFromEffectiveUser = readFrom.getEffectiveSubject().getUser();
        assertThat(readFromEffectiveUser, not(sameInstance(authentication.getEffectiveSubject().getUser())));
        assertThat(readFromEffectiveUser, equalTo(authentication.getEffectiveSubject().getUser()));

        User readFromAuthenticatingUser = readFrom.getAuthenticatingSubject().getUser();
        assertThat(readFromAuthenticatingUser, is(notNullValue()));
        assertThat(readFromAuthenticatingUser, not(sameInstance(authentication.getAuthenticatingSubject().getUser())));
        assertThat(readFromAuthenticatingUser, equalTo(authentication.getAuthenticatingSubject().getUser()));
    }

    public void testWriteToAndReadFromWithCrossClusterAccess() throws Exception {
        final Authentication authentication = AuthenticationTestHelper.builder().crossClusterAccess().build();
        assertThat(authentication.isCrossClusterAccess(), is(true));

        BytesStreamOutput output = new BytesStreamOutput();
        authentication.writeTo(output);
        final Authentication readFrom = new Authentication(output.bytes().streamInput());
        assertThat(readFrom.isCrossClusterAccess(), is(true));

        assertThat(readFrom, not(sameInstance(authentication)));
        assertThat(readFrom, equalTo(authentication));
    }

    public void testWriteToWithCrossClusterAccessThrowsOnUnsupportedVersion() throws Exception {
        final Authentication authentication = randomBoolean()
            ? AuthenticationTestHelper.builder().crossClusterAccess().build()
            : AuthenticationTestHelper.builder().build();

        final BytesStreamOutput out = new BytesStreamOutput();
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_0_0,
            TransportVersionUtils.getPreviousVersion(RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)
        );
        out.setTransportVersion(version);

        if (authentication.isCrossClusterAccess()) {
            final var ex = expectThrows(IllegalArgumentException.class, () -> authentication.writeTo(out));
            assertThat(
                ex.getMessage(),
                containsString(
                    "versions of Elasticsearch before ["
                        + RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                        + "] can't handle cross cluster access authentication and attempted to send to ["
                        + out.getTransportVersion().toReleaseVersion()
                        + "]"
                )
            );
        } else {
            authentication.writeTo(out);
            final StreamInput in = out.bytes().streamInput();
            in.setTransportVersion(out.getTransportVersion());
            final Authentication readFrom = new Authentication(in);
            assertThat(readFrom, equalTo(authentication.maybeRewriteForOlderVersion(out.getTransportVersion())));
        }
    }

    public void testWriteToAndReadFromWithCloudApiKeyAuthentication() throws Exception {
        final Authentication authentication = Authentication.newCloudApiKeyAuthentication(
            AuthenticationResult.success(
                new User(randomAlphanumericOfLength(5), "superuser"),
                Map.of(AuthenticationField.API_KEY_ID_KEY, "test-api-key-id")
            ),
            randomAlphanumericOfLength(10)
        );

        assertThat(authentication.isCloudApiKey(), is(true));

        BytesStreamOutput output = new BytesStreamOutput();
        authentication.writeTo(output);
        final Authentication readFrom = new Authentication(output.bytes().streamInput());
        assertThat(readFrom.isCloudApiKey(), is(true));

        assertThat(readFrom, not(sameInstance(authentication)));
        assertThat(readFrom, equalTo(authentication));
    }

    public void testWriteToWithCloudApiKeyThrowsOnUnsupportedVersion() {
        final Authentication authentication = Authentication.newCloudApiKeyAuthentication(
            AuthenticationResult.success(
                new User(randomAlphanumericOfLength(5), "superuser"),
                Map.of(AuthenticationField.API_KEY_ID_KEY, "test-api-key-id")
            ),
            randomAlphanumericOfLength(10)
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            final TransportVersion version = TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersions.V_8_0_0,
                TransportVersionUtils.getPreviousVersion(TransportVersions.SECURITY_CLOUD_API_KEY_REALM_AND_TYPE)
            );
            out.setTransportVersion(version);

            final var ex = expectThrows(IllegalArgumentException.class, () -> authentication.writeTo(out));
            assertThat(
                ex.getMessage(),
                containsString(
                    "versions of Elasticsearch before ["
                        + TransportVersions.SECURITY_CLOUD_API_KEY_REALM_AND_TYPE.toReleaseVersion()
                        + "] can't handle cloud API key authentication and attempted to send to ["
                        + out.getTransportVersion().toReleaseVersion()
                        + "]"
                )
            );
        }

    }

    public void testSystemUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(InternalUsers.SYSTEM_USER, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(InternalUsers.SYSTEM_USER)));
    }

    public void testXPackUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(InternalUsers.XPACK_USER, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(InternalUsers.XPACK_USER)));
    }

    public void testAsyncSearchUserReadAndWrite() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();

        AuthenticationSerializationHelper.writeUserTo(InternalUsers.ASYNC_SEARCH_USER, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertThat(readFrom, is(sameInstance(InternalUsers.ASYNC_SEARCH_USER)));
    }

    public void testFakeInternalUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeBoolean(true);
        output.writeString(randomAlphaOfLengthBetween(4, 30));
        try {
            AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());
            fail("system user had wrong name");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    public void testReservedUserSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        final ElasticUser elasticUser = new ElasticUser(true);
        AuthenticationSerializationHelper.writeUserTo(elasticUser, output);
        User readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertEquals(elasticUser, readFrom);

        final KibanaUser kibanaUser = new KibanaUser(true);
        output = new BytesStreamOutput();
        AuthenticationSerializationHelper.writeUserTo(kibanaUser, output);
        readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertEquals(kibanaUser, readFrom);

        final KibanaSystemUser kibanaSystemUser = new KibanaSystemUser(true);
        output = new BytesStreamOutput();
        AuthenticationSerializationHelper.writeUserTo(kibanaSystemUser, output);
        readFrom = AuthenticationSerializationHelper.readUserFrom(output.bytes().streamInput());

        assertEquals(kibanaSystemUser, readFrom);
    }

    public void testRolesRemovedFromUserForLegacyApiKeys() throws IOException {
        TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_7_0_0,
            TransportVersions.V_7_8_0
        );
        Subject authenticatingSubject = new Subject(
            new User("foo", "role"),
            new Authentication.RealmRef(AuthenticationField.API_KEY_REALM_NAME, AuthenticationField.API_KEY_REALM_TYPE, "node"),
            transportVersion,
            Map.of(AuthenticationField.API_KEY_ID_KEY, "abc")
        );
        Subject effectiveSubject = new Subject(
            new User("bar", "role"),
            new Authentication.RealmRef("native", "native", "node"),
            transportVersion,
            Map.of()
        );

        {
            Authentication actual = AuthenticationContextSerializer.decode(
                Authentication.doEncode(authenticatingSubject, authenticatingSubject, Authentication.AuthenticationType.API_KEY)
            );
            assertThat(actual.getAuthenticatingSubject().getUser().roles(), is(emptyArray()));
        }

        {
            Authentication actual = AuthenticationContextSerializer.decode(
                Authentication.doEncode(effectiveSubject, authenticatingSubject, Authentication.AuthenticationType.API_KEY)
            );
            assertThat(actual.getAuthenticatingSubject().getUser().roles(), is(emptyArray()));
            assertThat(actual.getEffectiveSubject().getUser().roles(), is(arrayContaining("role")));
        }

        {
            // do not strip roles for authentication methods other than API key
            Authentication actual = AuthenticationContextSerializer.decode(
                Authentication.doEncode(effectiveSubject, effectiveSubject, Authentication.AuthenticationType.REALM)
            );
            assertThat(actual.getAuthenticatingSubject().getUser().roles(), is(arrayContaining("role")));
        }
    }
}
