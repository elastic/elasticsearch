/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class SecurityContextTests extends ESTestCase {

    private Settings settings;
    private ThreadContext threadContext;
    private SecurityContext securityContext;

    @Before
    public void buildSecurityContext() throws IOException {
        settings = Settings.builder()
                .put("path.home", createTempDir())
                .build();
        threadContext = new ThreadContext(settings);
        securityContext = new SecurityContext(settings, threadContext);
    }

    public void testGetAuthenticationAndUserInEmptyContext() throws IOException {
        assertNull(securityContext.getAuthentication());
        assertNull(securityContext.getUser());
    }

    public void testGetAuthenticationAndUser() throws IOException {
        final User user = new User("test");
        final Authentication authentication = new Authentication(user, new RealmRef("ldap", "foo", "node1"), null);
        authentication.writeToContext(threadContext);

        assertEquals(authentication, securityContext.getAuthentication());
        assertEquals(user, securityContext.getUser());
    }

    public void testGetAuthenticationDoesNotSwallowIOException() {
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, ""); // an intentionally corrupt header
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        final UncheckedIOException e = expectThrows(UncheckedIOException.class, securityContext::getAuthentication);
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(EOFException.class));
    }

    public void testSetUser() {
        final User user = new User("test");
        assertNull(securityContext.getAuthentication());
        assertNull(securityContext.getUser());
        securityContext.setUser(user, Version.CURRENT);
        assertEquals(user, securityContext.getUser());
        assertEquals(AuthenticationType.INTERNAL, securityContext.getAuthentication().getAuthenticationType());

        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> securityContext.setUser(randomFrom(user, SystemUser.INSTANCE), Version.CURRENT));
        assertEquals("authentication is already present in the context", e.getMessage());
    }

    public void testExecuteAsUser() throws IOException {
        final User original;
        if (randomBoolean()) {
            original = new User("test");
            final Authentication authentication = new Authentication(original, new RealmRef("ldap", "foo", "node1"), null);
            authentication.writeToContext(threadContext);
        } else {
            original = null;
        }

        final User executionUser = new User("executor");
        final AtomicReference<StoredContext> contextAtomicReference = new AtomicReference<>();
        securityContext.executeAsUser(executionUser, (originalCtx) -> {
            assertEquals(executionUser, securityContext.getUser());
            assertEquals(AuthenticationType.INTERNAL, securityContext.getAuthentication().getAuthenticationType());
            contextAtomicReference.set(originalCtx);
        }, Version.CURRENT);

        final User userAfterExecution = securityContext.getUser();
        assertEquals(original, userAfterExecution);
        if (original != null) {
            assertEquals(AuthenticationType.REALM, securityContext.getAuthentication().getAuthenticationType());
        }
        StoredContext originalContext = contextAtomicReference.get();
        assertNotNull(originalContext);
        originalContext.restore();
        assertEquals(original, securityContext.getUser());
    }

    public void testExecuteAfterRewritingAuthentication() throws IOException {
        User user = new User("test", null, new User("authUser"));
        RealmRef authBy = new RealmRef("ldap", "foo", "node1");
        final Authentication original = new Authentication(user, authBy, authBy);
        original.writeToContext(threadContext);

        final AtomicReference<StoredContext> contextAtomicReference = new AtomicReference<>();
        securityContext.executeAfterRewritingAuthentication(originalCtx -> {
            Authentication authentication = securityContext.getAuthentication();
            assertEquals(original.getUser(), authentication.getUser());
            assertEquals(original.getAuthenticatedBy(), authentication.getAuthenticatedBy());
            assertEquals(original.getLookedUpBy(), authentication.getLookedUpBy());
            assertEquals(VersionUtils.getPreviousVersion(), authentication.getVersion());
            assertEquals(original.getAuthenticationType(), securityContext.getAuthentication().getAuthenticationType());
            contextAtomicReference.set(originalCtx);
        }, VersionUtils.getPreviousVersion());

        final Authentication authAfterExecution = securityContext.getAuthentication();
        assertEquals(original, authAfterExecution);
        StoredContext originalContext = contextAtomicReference.get();
        assertNotNull(originalContext);
        originalContext.restore();
        assertEquals(original, securityContext.getAuthentication());
    }
}
