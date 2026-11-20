/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The counterpart of {@code AsyncSearchSecurityTests}: the same cases against the stored headers of a stream.
 */
public class StreamSearchSecurityTests extends ESTestCase {

    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private StreamSearchSecurity security;
    /** What the privilege lookup answers. {@code null} makes it fail. */
    private volatile Boolean cancelTaskPrivilege = false;

    @Before
    public void setUpSecurity() {
        threadPool = new TestThreadPool(getTestName());
        threadContext = threadPool.getThreadContext();
        security = security(threadContext);
    }

    @After
    public void tearDownSecurity() {
        terminate(threadPool);
    }

    /**
     * The security plugin is not loaded in unit tests, so the one request it would answer, "has the current user the cancel_task
     * privilege", is answered by this client. Everything else is the real {@link SecurityContext}.
     */
    private StreamSearchSecurity security(ThreadContext context) {
        return new StreamSearchSecurity(new SecurityContext(Settings.EMPTY, context), new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked") // the response type of HasPrivilegesAction is HasPrivilegesResponse
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action, equalTo(HasPrivilegesAction.INSTANCE));
                HasPrivilegesRequest hasPrivileges = (HasPrivilegesRequest) request;
                assertThat(hasPrivileges.clusterPrivileges(), arrayContaining("cancel_task"));
                Boolean answer = cancelTaskPrivilege;
                if (answer == null) {
                    listener.onFailure(new IllegalStateException("simulated failure of the privilege lookup"));
                } else {
                    listener.onResponse(
                        (Response) new HasPrivilegesResponse(hasPrivileges.username(), answer, Map.of(), List.of(), Map.of())
                    );
                }
            }
        });
    }

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

    // The access rule: the stored headers of the creator against the current authentication

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
        StreamSearchSecurity asAuthenticatingUser = security(other);
        assertThat(asAuthenticatingUser.currentUserHasAccessToStreamWithHeaders(storedHeaders), is(false));
    }

    // The check a read and a delete make, with and without the expiry

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

    public void testAccessWithoutExpiryCheck() throws IOException {
        Authentication original = realmAuthentication("test", "realm");
        realmAuthentication("test", "realm").writeToContext(threadContext);
        long now = System.currentTimeMillis();
        StreamMetadata expired = metadata(headers(original), now - 1);

        expectThrows(ResourceNotFoundException.class, () -> security.getStreamAndCheckAuthentication("id", expired, now));
        // a delete does not look at the expiry
        assertThat(security.getStreamAndCheckAuthentication("id", expired), sameInstance(expired));

        // it still looks at the owner, and at whether there is a stream at all
        StreamMetadata ofSomeoneElse = metadata(headers(realmAuthentication("other", "realm")), now - 1);
        ResourceNotFoundException denied = expectThrows(
            ResourceNotFoundException.class,
            () -> security.getStreamAndCheckAuthentication("id", ofSomeoneElse)
        );
        ResourceNotFoundException unknown = expectThrows(
            ResourceNotFoundException.class,
            () -> security.getStreamAndCheckAuthentication("id", null)
        );
        assertThat(denied.getMessage(), equalTo(unknown.getMessage()));
    }

    // The cancel_task privilege

    private boolean hasCancelTaskPrivilege() {
        AtomicReference<Boolean> result = new AtomicReference<>();
        security.currentUserHasCancelTaskPrivilege(result::set);
        // the client of this test answers on the calling thread
        assertThat(result.get(), notNullValue());
        return result.get();
    }

    public void testCancelTaskPrivilege() throws IOException {
        realmAuthentication("test", "realm").writeToContext(threadContext);
        cancelTaskPrivilege = true;
        assertThat(hasCancelTaskPrivilege(), is(true));
        cancelTaskPrivilege = false;
        assertThat(hasCancelTaskPrivilege(), is(false));
    }

    public void testPrivilegeLookupFailureMeansNoPrivilege() throws IOException {
        realmAuthentication("test", "realm").writeToContext(threadContext);
        cancelTaskPrivilege = null;
        assertThat(hasCancelTaskPrivilege(), is(false));
    }

    public void testNoPrivilegeWithoutAuthentication() {
        // nothing was written to the thread context, and the privilege is not even looked up
        cancelTaskPrivilege = true;
        assertThat(hasCancelTaskPrivilege(), is(false));
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
