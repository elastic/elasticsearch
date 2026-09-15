/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SecurityRestContentTypePolicyTests extends ESTestCase {

    private ThreadContext threadContext;

    @Before
    public void init() throws Exception {
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testAllowsBrowserSafelistedContentTypeRequiresSecurityEnabled() throws Exception {
        var policy = new SecurityRestContentTypePolicy(true, true, threadContext);

        Authentication authentication = AuthenticationTestHelper.builder().realm().build(false);
        authentication.writeToContext(threadContext);
        RestRequest request = mock(RestRequest.class);

        assertThat(policy.allowsBrowserSafelistedContentType(request), is(true));

        policy = new SecurityRestContentTypePolicy(false, true, threadContext);
        assertThat(policy.allowsBrowserSafelistedContentType(request), is(false));
    }

    public void testAllowsBrowserSafelistedContentTypeRequiresHttpSslEnabled() throws Exception {
        Authentication authentication = AuthenticationTestHelper.builder().realm().build(false);
        authentication.writeToContext(threadContext);
        RestRequest request = mock(RestRequest.class);

        var policy = new SecurityRestContentTypePolicy(true, false, threadContext);
        assertThat(policy.allowsBrowserSafelistedContentType(request), is(false));
    }

    public void testAllowsBrowserSafelistedContentTypeRequiresAuthenticatedUser() {
        var policy = new SecurityRestContentTypePolicy(true, true, threadContext);

        RestRequest request = mock(RestRequest.class);
        assertThat(policy.allowsBrowserSafelistedContentType(request), is(false));
    }

    public void testAllowsBrowserSafelistedContentTypeRejectsAnonymousAuthentication() throws Exception {
        var policy = new SecurityRestContentTypePolicy(true, true, threadContext);

        Authentication authentication = AuthenticationTestHelper.builder().anonymous().build(false);
        authentication.writeToContext(threadContext);
        RestRequest request = mock(RestRequest.class);

        assertThat(policy.allowsBrowserSafelistedContentType(request), is(false));
    }
}
