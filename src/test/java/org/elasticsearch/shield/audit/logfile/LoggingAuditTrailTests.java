/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.shield.audit.logfile.CapturingLogger.Level;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
@Repeat(iterations = 10)
public class LoggingAuditTrailTests extends ElasticsearchTestCase {

    private static enum RestContent {

        VALID() {
            @Override
            protected boolean hasContent() {
                return true;
            }

            @Override
            protected BytesReference content() {
                return new BytesArray("{ \"key\": \"value\"}");
            }

            @Override
            protected String expectedMessage() {
                return "{ \"key\": \"value\"}";
            }
        },
        INVALID() {
            @Override
            protected boolean hasContent() {
                return true;
            }

            @Override
            protected BytesReference content() {
                return new BytesArray("{ \"key\": \"value\"");
            }

            @Override
            protected String expectedMessage() {
                return "{ \"key\": \"value\"";
            }
        },
        EMPTY() {
            @Override
            protected boolean hasContent() {
                return false;
            }

            @Override
            protected BytesReference content() {
                throw new RuntimeException("Should never be called");
            }

            @Override
            protected String expectedMessage() {
                return "";
            }
        };

        protected abstract boolean hasContent();
        protected abstract BytesReference content();
        protected abstract String expectedMessage();
    }

    @Test
    public void testAnonymousAccess_Transport() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            TransportMessage message = randomBoolean() ? new MockMessage() : new MockIndicesRequest();
            auditTrail.anonymousAccess("_action", message);
            switch (level) {
                case ERROR:
                    assertEmptyLog(logger);
                    break;
                case WARN:
                case INFO:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.WARN, "ANONYMOUS_ACCESS\thost=[local[_host]], action=[_action], indices=[idx1,idx2]");
                    } else {
                        assertMsg(logger, Level.WARN, "ANONYMOUS_ACCESS\thost=[local[_host]], action=[_action]");
                    }
                    break;
                case DEBUG:
                case TRACE:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.DEBUG, "ANONYMOUS_ACCESS\thost=[local[_host]], action=[_action], indices=[idx1,idx2], request=[MockIndicesRequest]");
                    } else {
                        assertMsg(logger, Level.DEBUG, "ANONYMOUS_ACCESS\thost=[local[_host]], action=[_action], request=[MockMessage]");
                    }
            }
        }
    }

    @Test
    public void testAnonymousAccess_Rest() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(request.getRemoteAddress()).thenReturn(new InetSocketAddress("_hostname", 9200));
        when(request.uri()).thenReturn("_uri");
        String expectedMessage = prepareRestContent(request);

        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.anonymousAccess(request);
            switch (level) {
                case ERROR:
                    assertEmptyLog(logger);
                    break;
                case WARN:
                case INFO:
                    assertMsg(logger, Level.WARN, "ANONYMOUS_ACCESS\thost=[_hostname:9200], URI=[_uri]");
                    break;
                case DEBUG:
                case TRACE:
                    assertMsg(logger, Level.DEBUG, "ANONYMOUS_ACCESS\thost=[_hostname:9200], URI=[_uri], request=[" + expectedMessage + "]");
            }
        }
    }

    @Test
    public void testAuthenticationFailed() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            TransportMessage message = randomBoolean() ? new MockMessage() : new MockIndicesRequest();
            auditTrail.authenticationFailed(new MockToken(), "_action", message);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.ERROR, "AUTHENTICATION_FAILED\thost=[local[_host]], principal=[_principal], action=[_action], indices=[idx1,idx2]");
                    } else {
                        assertMsg(logger, Level.ERROR, "AUTHENTICATION_FAILED\thost=[local[_host]], principal=[_principal], action=[_action]");
                    }
                    break;
                case DEBUG:
                case TRACE:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.DEBUG, "AUTHENTICATION_FAILED\thost=[local[_host]], principal=[_principal], action=[_action], indices=[idx1,idx2], request=[MockIndicesRequest]");
                    } else {
                        assertMsg(logger, Level.DEBUG, "AUTHENTICATION_FAILED\thost=[local[_host]], principal=[_principal], action=[_action], request=[MockMessage]");
                    }
            }
        }
    }

    @Test
    public void testAuthenticationFailed_Rest() throws Exception {
        for (Level level : Level.values()) {
            RestRequest request = mock(RestRequest.class);
            when(request.getRemoteAddress()).thenReturn(new InetSocketAddress("_hostname", 9200));
            when(request.uri()).thenReturn("_uri");
            String expectedMessage = prepareRestContent(request);
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.authenticationFailed(new MockToken(), request);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                    assertMsg(logger, Level.ERROR, "AUTHENTICATION_FAILED\thost=[_hostname:9200], principal=[_principal], URI=[_uri]");
                    break;
                case DEBUG:
                case TRACE:
                    assertMsg(logger, Level.DEBUG, "AUTHENTICATION_FAILED\thost=[_hostname:9200], principal=[_principal], URI=[_uri], request=[" + expectedMessage + "]");
            }
        }
    }

    @Test
    public void testAuthenticationFailed_Realm() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            TransportMessage message = randomBoolean() ? new MockMessage() : new MockIndicesRequest();
            auditTrail.authenticationFailed("_realm", new MockToken(), "_action", message);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                case DEBUG:
                    assertEmptyLog(logger);
                    break;
                case TRACE:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.TRACE, "AUTHENTICATION_FAILED[_realm]\thost=[local[_host]], principal=[_principal], action=[_action], indices=[idx1,idx2], request=[MockIndicesRequest]");
                    } else {
                        assertMsg(logger, Level.TRACE, "AUTHENTICATION_FAILED[_realm]\thost=[local[_host]], principal=[_principal], action=[_action], request=[MockMessage]");
                    }
            }
        }
    }

    @Test
    public void testAuthenticationFailed_Realm_Rest() throws Exception {
        for (Level level : Level.values()) {
            RestRequest request = mock(RestRequest.class);
            when(request.getRemoteAddress()).thenReturn(new InetSocketAddress("_hostname", 9200));
            when(request.uri()).thenReturn("_uri");
            String expectedMessage = prepareRestContent(request);
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.authenticationFailed("_realm", new MockToken(), request);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                case DEBUG:
                    assertEmptyLog(logger);
                    break;
                case TRACE:
                    assertMsg(logger, Level.TRACE, "AUTHENTICATION_FAILED[_realm]\thost=[_hostname:9200], principal=[_principal], URI=[_uri], request=[" + expectedMessage + "]");
            }
        }
    }

    @Test
    public void testAccessGranted() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            TransportMessage message = randomBoolean() ? new MockMessage() : new MockIndicesRequest();
            auditTrail.accessGranted(new User.Simple("_username", "r1"), "_action", message);
            switch (level) {
                case ERROR:
                case WARN:
                    assertEmptyLog(logger);
                    break;
                case INFO:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.INFO, "ACCESS_GRANTED\thost=[local[_host]], principal=[_username], action=[_action], indices=[idx1,idx2]");
                    } else {
                        assertMsg(logger, Level.INFO, "ACCESS_GRANTED\thost=[local[_host]], principal=[_username], action=[_action]");
                    }
                    break;
                case DEBUG:
                case TRACE:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.DEBUG, "ACCESS_GRANTED\thost=[local[_host]], principal=[_username], action=[_action], indices=[idx1,idx2], request=[MockIndicesRequest]");
                    } else {
                        assertMsg(logger, Level.DEBUG, "ACCESS_GRANTED\thost=[local[_host]], principal=[_username], action=[_action], request=[MockMessage]");
                    }
            }
        }
    }

    @Test
    public void testAccessGranted_SystemAction() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            TransportMessage message = randomBoolean() ? new MockMessage() : new MockIndicesRequest();
            auditTrail.accessGranted(new User.Simple("_username", "r1"), "internal:_action", message);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                case DEBUG:
                    assertEmptyLog(logger);
                    break;
                case TRACE:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.TRACE, "ACCESS_GRANTED\thost=[local[_host]], principal=[_username], action=[internal:_action], indices=[idx1,idx2], request=[MockIndicesRequest]");
                    } else {
                        assertMsg(logger, Level.TRACE, "ACCESS_GRANTED\thost=[local[_host]], principal=[_username], action=[internal:_action], request=[MockMessage]");
                    }
            }
        }
    }

    @Test
    public void testAccessDenied() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            TransportMessage message = randomBoolean() ? new MockMessage() : new MockIndicesRequest();
            auditTrail.accessDenied(new User.Simple("_username", "r1"), "_action", message);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.ERROR, "ACCESS_DENIED\thost=[local[_host]], principal=[_username], action=[_action], indices=[idx1,idx2]");
                    } else {
                        assertMsg(logger, Level.ERROR, "ACCESS_DENIED\thost=[local[_host]], principal=[_username], action=[_action]");
                    }
                    break;
                case DEBUG:
                case TRACE:
                    if (message instanceof IndicesRequest) {
                        assertMsg(logger, Level.DEBUG, "ACCESS_DENIED\thost=[local[_host]], principal=[_username], action=[_action], indices=[idx1,idx2], request=[MockIndicesRequest]");
                    } else {
                        assertMsg(logger, Level.DEBUG, "ACCESS_DENIED\thost=[local[_host]], principal=[_username], action=[_action], request=[MockMessage]");
                    }
            }
        }
    }

    @Test
    public void testConnectionDenied() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            InetAddress inetAddress = InetAddress.getLocalHost();
            ShieldIpFilterRule rule = new ShieldIpFilterRule(false, "_all");
            auditTrail.connectionDenied(inetAddress, "default", rule);
            switch (level) {
                case ERROR:
                    assertMsg(logger, Level.ERROR, String.format(Locale.ROOT, "CONNECTION_DENIED\thost=[%s], profile=[%s], rule=[deny %s]",
                            inetAddress.getHostAddress(), "default", "_all"));
                    break;
                case WARN:
                case INFO:
                case DEBUG:
                case TRACE:
            }
        }
    }

    @Test
    public void testConnectionGranted() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            InetAddress inetAddress = InetAddress.getLocalHost();
            ShieldIpFilterRule rule = IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
            auditTrail.connectionGranted(inetAddress, "default", rule);
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                case DEBUG:
                    assertEmptyLog(logger);
                    break;
                case TRACE:
                    assertMsg(logger, Level.TRACE, String.format(Locale.ROOT,
                            "CONNECTION_GRANTED\thost=[%s], profile=[default], rule=[allow default:accept_all]",
                            inetAddress.getHostAddress()));
            }
        }
    }

    private void assertMsg(CapturingLogger logger, Level msgLevel, String msg) {
        List<CapturingLogger.Msg> output = logger.output(msgLevel);
        assertThat(output.size(), is(1));
        assertThat(output.get(0).text, equalTo(msg));
    }

    private void assertEmptyLog(CapturingLogger logger) {
        assertThat(logger.isEmpty(), is(true));
    }

    private String prepareRestContent(RestRequest mock) {
        RestContent content = randomFrom(RestContent.values());
        when(mock.hasContent()).thenReturn(content.hasContent());
        if (content.hasContent()) {
            when(mock.content()).thenReturn(content.content());
        }
        return content.expectedMessage();
    }

    private static class MockMessage extends TransportMessage<MockMessage> {

        private MockMessage() {
            remoteAddress(new LocalTransportAddress("_host"));
        }
    }

    private static class MockIndicesRequest extends TransportMessage<MockIndicesRequest> implements IndicesRequest {

        private MockIndicesRequest() {
            remoteAddress(new LocalTransportAddress("_host"));
        }

        @Override
        public String[] indices() {
            return new String[] { "idx1", "idx2" };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpandOpenAndForbidClosed();
        }

        @Override
        public String toString() {
            return "mock-message";
        }
    }

    private static class MockToken implements AuthenticationToken {
        @Override
        public String principal() {
            return "_principal";
        }

        @Override
        public Object credentials() {
            fail("it's not allowed to print the credentials of the auth token");
            return null;
        }

        @Override
        public void clearCredentials() {

        }
    }
}
