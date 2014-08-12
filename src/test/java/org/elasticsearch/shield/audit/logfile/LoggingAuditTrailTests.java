/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.logfile;

import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.shield.audit.logfile.CapturingLogger.Level;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class LoggingAuditTrailTests extends ElasticsearchTestCase {

    @Test
    public void testAnonymousAccess() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.anonymousAccess("_action", new MockMessage());
            switch (level) {
                case ERROR:
                    assertEmptyLog(logger);
                    break;
                case WARN:
                case INFO:
                    assertMsg(logger, Level.WARN, "ANONYMOUS_ACCESS\thost=[local[_host]], action=[_action]");
                    break;
                case DEBUG:
                case TRACE:
                    assertMsg(logger, Level.DEBUG, "ANONYMOUS_ACCESS\thost=[local[_host]], action=[_action], request=[mock-message]");
            }
        }
    }

    @Test
    public void testAuthenticationFailed() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.authenticationFailed(new MockToken(), "_action", new MockMessage());
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                    assertMsg(logger, Level.ERROR, "AUTHENTICATION_FAILED\thost=[local[_host]], action=[_action], principal=[_principal]");
                    break;
                case DEBUG:
                case TRACE:
                    assertMsg(logger, Level.DEBUG, "AUTHENTICATION_FAILED\thost=[local[_host]], action=[_action], principal=[_principal], request=[mock-message]");
            }
        }
    }

    @Test
    public void testAuthenticationFailed_Realm() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.authenticationFailed("_realm", new MockToken(), "_action", new MockMessage());
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                case DEBUG:
                    assertEmptyLog(logger);
                    break;
                case TRACE:
                    assertMsg(logger, Level.TRACE, "AUTHENTICATION_FAILED[_realm]\thost=[local[_host]], action=[_action], principal=[_principal], request=[mock-message]");
            }
        }
    }

    @Test
    public void testAccessGranted() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.accessGranted(new User.Simple("_username", "r1"), "_action", new MockMessage());
            switch (level) {
                case ERROR:
                case WARN:
                    assertEmptyLog(logger);
                    break;
                case INFO:
                    assertMsg(logger, Level.INFO, "ACCESS_GRANTED\thost=[local[_host]], action=[_action], principal=[_username]");
                    break;
                case DEBUG:
                case TRACE:
                    assertMsg(logger, Level.DEBUG, "ACCESS_GRANTED\thost=[local[_host]], action=[_action], principal=[_username], request=[mock-message]");
            }
        }
    }

    @Test
    public void testAccessDenied() throws Exception {
        for (Level level : Level.values()) {
            CapturingLogger logger = new CapturingLogger(level);
            LoggingAuditTrail auditTrail = new LoggingAuditTrail(logger);
            auditTrail.accessDenied(new User.Simple("_username", "r1"), "_action", new MockMessage());
            switch (level) {
                case ERROR:
                case WARN:
                case INFO:
                    assertMsg(logger, Level.ERROR, "ACCESS_DENIED\thost=[local[_host]], action=[_action], principal=[_username]");
                    break;
                case DEBUG:
                case TRACE:
                    assertMsg(logger, Level.DEBUG, "ACCESS_DENIED\thost=[local[_host]], action=[_action], principal=[_username], request=[mock-message]");
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

    private static class MockMessage extends TransportMessage<MockMessage> {

        private MockMessage() {
            remoteAddress(new LocalTransportAddress("_host"));
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
    }
}
