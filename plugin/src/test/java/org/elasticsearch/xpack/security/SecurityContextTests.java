/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class SecurityContextTests extends ESTestCase {

    private boolean signHeader;
    private Settings settings;
    private ThreadContext threadContext;
    private CryptoService cryptoService;
    private SecurityContext securityContext;

    @Before
    public void buildSecurityContext() throws IOException {
        signHeader = randomBoolean();
        settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(AuthenticationService.SIGN_USER_HEADER.getKey(), signHeader)
                .build();
        threadContext = new ThreadContext(settings);
        cryptoService = new CryptoService(settings, new Environment(settings));
        securityContext = new SecurityContext(settings, threadContext, cryptoService);
    }

    public void testGetAuthenticationAndUserInEmptyContext() throws IOException {
        assertNull(securityContext.getAuthentication());
        assertNull(securityContext.getUser());
    }

    public void testGetAuthenticationAndUser() throws IOException {
        final User user = new User("test");
        final Authentication authentication = new Authentication(user, new RealmRef("ldap", "foo", "node1"), null);
        authentication.writeToContext(threadContext, cryptoService, signHeader);

        assertEquals(authentication, securityContext.getAuthentication());
        assertEquals(user, securityContext.getUser());
    }

    public void testSetUser() {
        final User user = new User("test");
        assertNull(securityContext.getAuthentication());
        assertNull(securityContext.getUser());
        securityContext.setUser(user);
        assertEquals(user, securityContext.getUser());

        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> securityContext.setUser(randomFrom(user, SystemUser.INSTANCE)));
        assertEquals("authentication is already present in the context", e.getMessage());
    }

    public void testExecuteAsUser() throws IOException {
        final User original;
        if (randomBoolean()) {
            original = new User("test");
            final Authentication authentication = new Authentication(original, new RealmRef("ldap", "foo", "node1"), null);
            authentication.writeToContext(threadContext, cryptoService, signHeader);
        } else {
            original = null;
        }

        final User executionUser = new User("executor");
        final AtomicReference<StoredContext> contextAtomicReference = new AtomicReference<>();
        securityContext.executeAsUser(executionUser, (originalCtx) -> {
            assertEquals(executionUser, securityContext.getUser());
            contextAtomicReference.set(originalCtx);
        });

        final User userAfterExecution = securityContext.getUser();
        assertEquals(original, userAfterExecution);
        StoredContext originalContext = contextAtomicReference.get();
        assertNotNull(originalContext);
        originalContext.restore();
        assertEquals(original, securityContext.getUser());
    }
}
