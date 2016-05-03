/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.shield.audit.index.IndexAuditTrail.State;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessage;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IndexAuditTrailMutedTests extends ESTestCase {

    private InternalClient client;
    private TransportClient transportClient;
    private ThreadPool threadPool;
    private Transport transport;
    private IndexAuditTrail auditTrail;

    private AtomicBoolean messageEnqueued;
    private AtomicBoolean clientCalled;

    @Before
    public void setup() {
        transport = mock(Transport.class);
        when(transport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { DummyTransportAddress.INSTANCE },
                        DummyTransportAddress.INSTANCE));

        threadPool = new ThreadPool("index audit trail tests");
        transportClient = TransportClient.builder().settings(Settings.EMPTY).build();
        clientCalled = new AtomicBoolean(false);
        client = new InternalClient(transportClient) {
            @Override
            protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends
                    ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                    Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
                clientCalled.set(true);
            }
        };
        messageEnqueued = new AtomicBoolean(false);
    }

    @After
    public void stop() {
        if (auditTrail != null) {
            auditTrail.close();
        }
        if (transportClient != null) {
            transportClient.close();
        }
        threadPool.shutdown();
    }

    public void testAnonymousAccessDeniedMutedTransport() {
        createAuditTrail(new String[] { "anonymous_access_denied" });
        TransportMessage message = mock(TransportMessage.class);
        auditTrail.anonymousAccessDenied("_action", message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));

        verifyZeroInteractions(message);
    }

    public void testAnonymousAccessDeniedMutedRest() {
        createAuditTrail(new String[] { "anonymous_access_denied" });
        RestRequest restRequest = mock(RestRequest.class);
        auditTrail.anonymousAccessDenied(restRequest);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(restRequest);
    }

    public void testAuthenticationFailedMutedTransport() {
        createAuditTrail(new String[] { "authentication_failed" });
        TransportMessage message = mock(TransportMessage.class);
        AuthenticationToken token = mock(AuthenticationToken.class);

        // with realm
        auditTrail.authenticationFailed(randomAsciiOfLengthBetween(2, 10), token, "_action", message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));

        // without realm
        auditTrail.authenticationFailed(token, "_action", message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));

        // without the token
        auditTrail.authenticationFailed("_action", message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(token, message);
    }

    public void testAuthenticationFailedMutedRest() {
        createAuditTrail(new String[] { "authentication_failed" });
        RestRequest restRequest = mock(RestRequest.class);
        AuthenticationToken token = mock(AuthenticationToken.class);

        // with realm
        auditTrail.authenticationFailed(randomAsciiOfLengthBetween(2, 10), token, restRequest);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));

        // without the realm
        auditTrail.authenticationFailed(token, restRequest);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));

        // without the token
        auditTrail.authenticationFailed(restRequest);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(token, restRequest);
    }

    public void testAccessGrantedMuted() {
        createAuditTrail(new String[] { "access_granted" });
        TransportMessage message = mock(TransportMessage.class);
        User user = mock(User.class);
        auditTrail.accessGranted(user, randomAsciiOfLengthBetween(6, 40), message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(message, user);
    }

    public void testSystemAccessGrantedMuted() {
        createAuditTrail(randomFrom(new String[] { "access_granted" }, null));
        TransportMessage message = mock(TransportMessage.class);
        User user = SystemUser.INSTANCE;
        auditTrail.accessGranted(user, "internal:foo", message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(message);
    }

    public void testAccessDeniedMuted() {
        createAuditTrail(new String[] { "access_denied" });
        TransportMessage message = mock(TransportMessage.class);
        User user = mock(User.class);
        auditTrail.accessDenied(user, randomAsciiOfLengthBetween(6, 40), message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(message, user);
    }

    public void testTamperedRequestMuted() {
        createAuditTrail(new String[] { "tampered_request" });
        TransportMessage message = mock(TransportMessage.class);
        User user = mock(User.class);

        // with user
        auditTrail.tamperedRequest(user, randomAsciiOfLengthBetween(6, 40), message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));

        // without user
        auditTrail.tamperedRequest(randomAsciiOfLengthBetween(6, 40), message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(message, user);
    }

    public void testConnectionGrantedMuted() {
        createAuditTrail(new String[] { "connection_granted" });
        InetAddress address = mock(InetAddress.class);
        ShieldIpFilterRule rule = mock(ShieldIpFilterRule.class);

        auditTrail.connectionGranted(address, randomAsciiOfLengthBetween(1, 12), rule);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(address, rule);
    }

    public void testConnectionDeniedMuted() {
        createAuditTrail(new String[] { "connection_denied" });
        InetAddress address = mock(InetAddress.class);
        ShieldIpFilterRule rule = mock(ShieldIpFilterRule.class);

        auditTrail.connectionDenied(address, randomAsciiOfLengthBetween(1, 12), rule);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(address, rule);
    }

    public void testRunAsGrantedMuted() {
        createAuditTrail(new String[] { "run_as_granted" });
        TransportMessage message = mock(TransportMessage.class);
        User user = mock(User.class);

        auditTrail.runAsGranted(user, randomAsciiOfLengthBetween(6, 40), message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(message, user);
    }

    public void testRunAsDeniedMuted() {
        createAuditTrail(new String[] { "run_as_denied" });
        TransportMessage message = mock(TransportMessage.class);
        User user = mock(User.class);

        auditTrail.runAsDenied(user, randomAsciiOfLengthBetween(6, 40), message);
        assertThat(messageEnqueued.get(), is(false));
        assertThat(clientCalled.get(), is(false));
        
        verifyZeroInteractions(message, user);
    }

    IndexAuditTrail createAuditTrail(String[] excludes) {
        Settings settings = IndexAuditTrailTests.levelSettings(null, excludes);
        auditTrail = new IndexAuditTrail(settings, transport, Providers.of(client), threadPool, mock(ClusterService.class)) {
            @Override
            void putTemplate(Settings settings) {
                // make this a no-op so we don't have to stub out unnecessary client activities
            }

            @Override
            BlockingQueue<Message> createQueue(int maxQueueSize) {
                return new LinkedBlockingQueue<Message>(maxQueueSize) {
                    @Override
                    public boolean offer(Message message) {
                        messageEnqueued.set(true);
                        return super.offer(message);
                    }
                };
            }
        };
        auditTrail.start(true);
        assertThat(auditTrail.state(), is(State.STARTED));
        return auditTrail;
    }
}
