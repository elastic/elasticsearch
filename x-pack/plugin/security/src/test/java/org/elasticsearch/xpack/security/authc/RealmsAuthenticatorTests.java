/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.junit.Before;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RealmsAuthenticatorTests extends ESTestCase {

    private static final String SECOND_REALM_NAME = "second_realm";
    private static final String SECOND_REALM_TYPE = "second";
    private static final String FIRST_REALM_NAME = "file_realm";
    private static final String FIRST_REALM_TYPE = "file";

    private ThreadContext threadContext;
    private AuthenticationToken token;
    private InetSocketAddress remoteAddress;
    private TransportRequest transportRequest;
    private RestRequest restRequest;

    private Realms realms;
    private Realm firstRealm;
    private Realm secondRealm;
    private Authenticator.Context context;
    private RealmsAuthenticator realmsAuthenticator;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws Exception {

        token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext = new ThreadContext(Settings.EMPTY);
        transportRequest = new AuthenticationServiceTests.InternalRequest();
        remoteAddress = new InetSocketAddress(InetAddress.getLocalHost(), 100);
        transportRequest.remoteAddress(new TransportAddress(remoteAddress));
        restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress).build();

        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn(FIRST_REALM_TYPE);
        when(firstRealm.name()).thenReturn(FIRST_REALM_NAME);
        when(firstRealm.toString()).thenReturn(FIRST_REALM_NAME + "/" + FIRST_REALM_TYPE);
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn(SECOND_REALM_TYPE);
        when(secondRealm.name()).thenReturn(SECOND_REALM_NAME);
        when(secondRealm.toString()).thenReturn(SECOND_REALM_NAME + "/" + SECOND_REALM_TYPE);

        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.ALL_REALMS_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(Security.STANDARD_REALMS_FEATURE)).thenReturn(true);
        when(licenseState.checkFeature(XPackLicenseState.Feature.SECURITY_TOKEN_SERVICE)).thenReturn(true);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.checkFeature(XPackLicenseState.Feature.SECURITY_AUDITING)).thenReturn(true);
        when(licenseState.getOperationMode()).thenReturn(randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.PLATINUM));

        ReservedRealm reservedRealm = mock(ReservedRealm.class);
        when(reservedRealm.type()).thenReturn("reserved");
        when(reservedRealm.name()).thenReturn("reserved_realm");

        realms = spy(new AuthenticationServiceTests.TestRealms(
            Settings.EMPTY, TestEnvironment.newEnvironment(Settings.EMPTY),
            Map.of(FileRealmSettings.TYPE, config -> mock(FileRealm.class), NativeRealmSettings.TYPE, config -> mock(NativeRealm.class)),
            licenseState, threadContext, reservedRealm, Arrays.asList(firstRealm, secondRealm),
            Arrays.asList(firstRealm)));

        // Needed because this is calculated in the constructor, which means the override doesn't get called correctly
        realms.recomputeActiveRealms();
        assertThat(realms.getActiveRealms(), contains(firstRealm, secondRealm));

        context = mock(Authenticator.Context.class);
        when(context.getThreadContext()).thenReturn(threadContext);
        when(context.getDefaultOrderedRealmList()).thenReturn(realms.getActiveRealms());

        final String nodeName = randomAlphaOfLength(8);
        final AtomicLong numInvalidation = new AtomicLong();
        @SuppressWarnings("unchecked")
        final Cache<String, Realm> lastSuccessfulAuthCache =  Mockito.mock(Cache.class);
        realmsAuthenticator = new RealmsAuthenticator(nodeName, numInvalidation, lastSuccessfulAuthCache);
    }

}
