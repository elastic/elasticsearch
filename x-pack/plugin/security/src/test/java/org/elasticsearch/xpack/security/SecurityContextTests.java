/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.security.authc.Authentication.VERSION_API_KEY_ROLES_AS_BYTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SecurityContextTests extends ESTestCase {

    private Settings settings;
    private ThreadContext threadContext;
    private SecurityContext securityContext;

    @Before
    public void buildSecurityContext() throws IOException {
        settings = Settings.builder().put("path.home", createTempDir()).build();
        threadContext = new ThreadContext(settings);
        securityContext = new SecurityContext(settings, threadContext);
    }

    public void testGetAuthenticationAndUserInEmptyContext() throws IOException {
        assertNull(securityContext.getAuthentication());
        assertNull(securityContext.getUser());
    }

    public void testGetAuthenticationAndUser() throws IOException {
        final User user = new User("test");
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("ldap", "foo", "node1"))
            .build(false);
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

    public void testSetInternalUser() {
        final User internalUser = randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE);
        assertNull(securityContext.getAuthentication());
        assertNull(securityContext.getUser());
        securityContext.setInternalUser(internalUser, Version.CURRENT);
        assertEquals(internalUser, securityContext.getUser());
        assertEquals(AuthenticationType.INTERNAL, securityContext.getAuthentication().getAuthenticationType());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> securityContext.setInternalUser(randomFrom(internalUser, SystemUser.INSTANCE), Version.CURRENT)
        );
        assertEquals("authentication ([_xpack_security_authentication]) is already present in the context", e.getMessage());
    }

    public void testExecuteAsUser() throws IOException {
        final User original;
        if (randomBoolean()) {
            original = new User("test");
            final Authentication authentication = AuthenticationTestHelper.builder()
                .realm()
                .user(original)
                .realmRef(new RealmRef("ldap", "foo", "node1"))
                .build(false);
            authentication.writeToContext(threadContext);
        } else {
            original = null;
        }

        final User executionUser = randomFrom(
            SystemUser.INSTANCE,
            XPackUser.INSTANCE,
            XPackSecurityUser.INSTANCE,
            AsyncSearchUser.INSTANCE
        );
        final AtomicReference<StoredContext> contextAtomicReference = new AtomicReference<>();
        securityContext.executeAsInternalUser(executionUser, Version.CURRENT, (originalCtx) -> {
            assertEquals(executionUser, securityContext.getUser());
            assertEquals(AuthenticationType.INTERNAL, securityContext.getAuthentication().getAuthenticationType());
            contextAtomicReference.set(originalCtx);
        });

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
        RealmRef authBy = new RealmRef("ldap", "foo", "node1");
        final Authentication original = AuthenticationTestHelper.builder()
            .user(new User("authUser"))
            .realmRef(authBy)
            .runAs()
            .user(new User("test"))
            .realmRef(authBy)
            .build();
        original.writeToContext(threadContext);
        final Map<String, String> requestHeaders = Map.of(
            AuthenticationField.PRIVILEGE_CATEGORY_KEY,
            randomAlphaOfLengthBetween(3, 10),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            Task.X_OPAQUE_ID_HTTP_HEADER,
            randomAlphaOfLength(10),
            Task.TRACE_ID,
            randomAlphaOfLength(20)
        );
        threadContext.putHeader(requestHeaders);

        final AtomicReference<StoredContext> contextAtomicReference = new AtomicReference<>();
        securityContext.executeAfterRewritingAuthentication(originalCtx -> {
            Authentication authentication = securityContext.getAuthentication();
            assertEquals(original.getEffectiveSubject().getUser(), authentication.getEffectiveSubject().getUser());
            assertEquals(original.getAuthenticatingSubject().getRealm(), authentication.getAuthenticatingSubject().getRealm());
            assertEquals(original.getLookedUpBy(), authentication.getLookedUpBy());
            assertEquals(VersionUtils.getPreviousVersion(), authentication.getEffectiveSubject().getVersion());
            assertEquals(original.getAuthenticationType(), securityContext.getAuthentication().getAuthenticationType());
            contextAtomicReference.set(originalCtx);
            // Other request headers should be preserved
            requestHeaders.forEach((k, v) -> assertThat(threadContext.getHeader(k), equalTo(v)));
        }, VersionUtils.getPreviousVersion());

        final Authentication authAfterExecution = securityContext.getAuthentication();
        assertEquals(original, authAfterExecution);
        StoredContext originalContext = contextAtomicReference.get();
        assertNotNull(originalContext);
        originalContext.restore();
        assertEquals(original, securityContext.getAuthentication());
    }

    public void testExecuteAfterRewritingAuthenticationWillConditionallyRewriteNewApiKeyMetadata() throws IOException {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(AuthenticationField.API_KEY_ID_KEY, randomAlphaOfLengthBetween(1, 10));
        metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10));
        metadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}"));
        metadata.put(
            AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
            new BytesArray("{\"limitedBy role\": {\"cluster\": [\"all\"]}}")
        );

        final Authentication original = AuthenticationTestHelper.builder().apiKey().metadata(metadata).version(Version.V_8_0_0).build();
        original.writeToContext(threadContext);

        // If target is old node, rewrite new style API key metadata to old format
        securityContext.executeAfterRewritingAuthentication(originalCtx -> {
            Authentication authentication = securityContext.getAuthentication();
            assertEquals(
                Map.of("a role", Map.of("cluster", List.of("all"))),
                authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY)
            );
            assertEquals(
                Map.of("limitedBy role", Map.of("cluster", List.of("all"))),
                authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
            );
        }, Version.V_7_8_0);

        // If target is new node, no need to rewrite the new style API key metadata
        securityContext.executeAfterRewritingAuthentication(originalCtx -> {
            Authentication authentication = securityContext.getAuthentication();
            assertSame(original.getAuthenticatingSubject().getMetadata(), authentication.getAuthenticatingSubject().getMetadata());
        }, VersionUtils.randomVersionBetween(random(), VERSION_API_KEY_ROLES_AS_BYTES, Version.CURRENT));
    }

    public void testExecuteAfterRewritingAuthenticationWillConditionallyRewriteOldApiKeyMetadata() throws IOException {
        final Authentication original = AuthenticationTestHelper.builder().apiKey().version(Version.V_7_8_0).build();

        // original authentication has the old style of role descriptor maps
        assertThat(
            original.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY),
            instanceOf(Map.class)
        );
        assertThat(
            original.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
            instanceOf(Map.class)
        );

        original.writeToContext(threadContext);

        // If target is old node, no need to rewrite old style API key metadata
        securityContext.executeAfterRewritingAuthentication(originalCtx -> {
            Authentication authentication = securityContext.getAuthentication();
            assertSame(original.getAuthenticatingSubject().getMetadata(), authentication.getAuthenticatingSubject().getMetadata());
        }, Version.V_7_8_0);

        // If target is new node, ensure old map style API key metadata is rewritten to bytesreference
        securityContext.executeAfterRewritingAuthentication(originalCtx -> {
            Authentication authentication = securityContext.getAuthentication();
            List.of(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY)
                .forEach(key -> {
                    assertThat(authentication.getAuthenticatingSubject().getMetadata().get(key), instanceOf(BytesReference.class));

                    assertThat(
                        XContentHelper.convertToMap(
                            (BytesReference) authentication.getAuthenticatingSubject().getMetadata().get(key),
                            false,
                            XContentType.JSON
                        ).v2(),
                        equalTo(original.getAuthenticatingSubject().getMetadata().get(key))
                    );
                });
        }, VersionUtils.randomVersionBetween(random(), VERSION_API_KEY_ROLES_AS_BYTES, Version.CURRENT));
    }
}
