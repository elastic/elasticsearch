/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultAuthenticationFailureHandlerTests extends ESTestCase {
    private static final String NEGOTIATE_SCHEME = "Negotiate";
    private Realm realmWithBasicAuthScheme;
    private Realm realmWithNegotiateAuthScheme;
    private List<Realm> listRealms;
    private Realms realms;

    @Before
    public void setup() {
        realmWithBasicAuthScheme = mock(Realm.class);
        when(realmWithBasicAuthScheme.getWWWAuthenticateHeaderValue()).thenReturn(Realm.WWW_AUTHN_HEADER_DEFAULT_VALUE);
        realmWithNegotiateAuthScheme = mock(Realm.class);
        when(realmWithNegotiateAuthScheme.getWWWAuthenticateHeaderValue()).thenReturn(NEGOTIATE_SCHEME);
        listRealms = new ArrayList<>();
        listRealms.add(realmWithBasicAuthScheme);
        listRealms.add(realmWithNegotiateAuthScheme);
        realms = mock(Realms.class);
    }

    public void testDefaultAuthenticationFailureHandlerReturnsCorrectSchemeBasedOnOrder() {

        when(realmWithBasicAuthScheme.order()).thenReturn(randomIntBetween(0, 5));
        when(realmWithNegotiateAuthScheme.order()).thenReturn(randomIntBetween(0, 5));
        Collections.sort(listRealms, (o1, o2) -> {
            return o1.order() - o2.order();
        });
        when(realms.asList()).thenReturn(listRealms);
        final Map<String, String[]> defaultResponseHeaders = new HashMap<>();
        defaultResponseHeaders.put(Realm.WWW_AUTHN_HEADER,
                new String[] { listRealms.get(listRealms.size() - 1).getWWWAuthenticateHeaderValue() });
        DefaultAuthenticationFailureHandler failureHandler = new DefaultAuthenticationFailureHandler(defaultResponseHeaders);
        ElasticsearchSecurityException ese = failureHandler.failedAuthentication(new FakeRestRequest(), mock(AuthenticationToken.class),
                new ThreadContext(Settings.builder().build()));

        assertNotNull(ese);
        assertNotNull(ese.getHeader(Realm.WWW_AUTHN_HEADER));
        assertEquals(listRealms.get(listRealms.size() - 1).getWWWAuthenticateHeaderValue(),
                ese.getHeader(Realm.WWW_AUTHN_HEADER).get(0));
    }

}
