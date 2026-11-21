/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The counterpart of {@code AsyncSearchSecurityTests}: the same cases against the stored headers of a stream.
 */
public class StreamSearchSecurityTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final StreamSearchSecurity security = new StreamSearchSecurity(new SecurityContext(Settings.EMPTY, threadContext));

    private static Authentication realmAuthentication(String user, String realm) {
        return realmAuthentication(user, realm, "file");
    }

    private static Authentication realmAuthentication(String user, String realm, String realmType) {
        return Authentication.newRealmAuthentication(new User(user, "role"), new Authentication.RealmRef(realm, realmType, "node"));
    }

    private static Map<String, String> headers(Authentication authentication) throws IOException {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        authentication.writeToContext(context);
        return context.getHeaders();
    }

    public void testSameUserHasAccess() throws IOException {
        Authentication original = realmAuthentication("test", "realm");
        // an equal authentication that is a different instance, as a second request would have
        realmAuthentication("test", "realm").writeToContext(threadContext);
        assertThat(security.currentUserHasAccessToStreamWithHeaders(headers(original)), is(true));
    }

    public void testStoredHeadersEmptyMeansSecurityWasOff() throws IOException {
        realmAuthentication("test", "realm").writeToContext(threadContext);
        assertThat(security.currentUserHasAccessToStreamWithHeaders(Map.of()), is(true));
    }

    public void testCurrentRequestNotAuthenticated() throws IOException {
        Authentication original = realmAuthentication("test", "realm");
        // nothing was written to the thread context: the current request is not authenticated
        assertThat(security.currentUserHasAccessToStreamWithHeaders(headers(original)), is(false));
        assertThat(security.currentUserHasAccessToStreamWithHeaders(Map.of()), is(true));
    }

    public void testDifferentUserHasNoAccess() throws IOException {
        Authentication original = realmAuthentication("test", "realm");
        realmAuthentication("other", "realm").writeToContext(threadContext);
        assertThat(security.currentUserHasAccessToStreamWithHeaders(headers(original)), is(false));
    }

    public void testSameNameInAnotherRealmTypeHasNoAccess() throws IOException {
        Authentication original = realmAuthentication("test", "realm", "file");
        realmAuthentication("test", "realm", "ldap").writeToContext(threadContext);
        assertThat(security.currentUserHasAccessToStreamWithHeaders(headers(original)), is(false));
    }

    public void testRunAs() throws IOException {
        User authenticatingUser = new User("authenticated", "runas");
        User effectiveUser = new User("test", "role");
        Authentication.RealmRef realm = new Authentication.RealmRef("realm", "file", "node");
        Authentication runAs = Authentication.newRealmAuthentication(authenticatingUser, realm).runAs(effectiveUser, realm);
        // the stream was created while running as "test"
        Map<String, String> storedHeaders = headers(runAs);

        // "test" reading directly may read what was created while running as "test"
        realmAuthentication("test", "realm").writeToContext(threadContext);
        assertThat(security.currentUserHasAccessToStreamWithHeaders(storedHeaders), is(true));

        // the authenticating user alone may not
        ThreadContext other = new ThreadContext(Settings.EMPTY);
        Authentication.newRealmAuthentication(authenticatingUser, realm).writeToContext(other);
        StreamSearchSecurity asAuthenticatingUser = new StreamSearchSecurity(new SecurityContext(Settings.EMPTY, other));
        assertThat(asAuthenticatingUser.currentUserHasAccessToStreamWithHeaders(storedHeaders), is(false));
    }

    public void testGetStreamAndCheckAuthentication() throws IOException {
        Authentication original = realmAuthentication("test", "realm");
        realmAuthentication("test", "realm").writeToContext(threadContext);
        long now = System.currentTimeMillis();
        StreamMetadata allowed = metadata(headers(original), now + 60_000);
        assertThat(security.getStreamAndCheckAuthentication("id", allowed, now), sameInstance(allowed));
    }

    public void testUnknownExpiredAndDeniedGiveTheSameNotFound() throws IOException {
        Authentication original = realmAuthentication("test", "realm");
        realmAuthentication("other", "realm").writeToContext(threadContext);
        long now = System.currentTimeMillis();

        ResourceNotFoundException unknown = expectThrows(
            ResourceNotFoundException.class,
            () -> security.getStreamAndCheckAuthentication("id", null, now)
        );
        ResourceNotFoundException expired = expectThrows(
            ResourceNotFoundException.class,
            () -> security.getStreamAndCheckAuthentication("id", metadata(Map.of(), now - 1), now)
        );
        ResourceNotFoundException denied = expectThrows(
            ResourceNotFoundException.class,
            () -> security.getStreamAndCheckAuthentication("id", metadata(headers(original), now + 60_000), now)
        );
        // a caller cannot tell an unknown stream from one that belongs to someone else
        assertThat(expired.getMessage(), equalTo(unknown.getMessage()));
        assertThat(denied.getMessage(), equalTo(unknown.getMessage()));
        assertThat(denied.status(), equalTo(unknown.status()));
    }

    private static StreamMetadata metadata(Map<String, String> headers, long expirationMillis) {
        return new StreamMetadata(
            "id",
            new TaskId("node", 1),
            Instant.ofEpochMilli(expirationMillis),
            List.of(),
            headers,
            List.of(),
            StreamMetadata.Status.RUNNING,
            null
        );
    }
}
