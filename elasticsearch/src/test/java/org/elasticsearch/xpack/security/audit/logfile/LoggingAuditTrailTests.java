/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.FakeRestRequest.Builder;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoggingAuditTrailTests extends ESTestCase {

    private enum RestContent {
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
                throw new RuntimeException("should never be called");
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

    private String prefix;
    private Settings settings;
    private DiscoveryNode localNode;
    private ClusterService clusterService;
    private ThreadContext threadContext;
    private boolean includeRequestBody;

    @Before
    public void init() throws Exception {
        includeRequestBody = randomBoolean();
        settings = Settings.builder()
                .put("xpack.security.audit.logfile.prefix.emit_node_host_address", randomBoolean())
                .put("xpack.security.audit.logfile.prefix.emit_node_host_name", randomBoolean())
                .put("xpack.security.audit.logfile.prefix.emit_node_name", randomBoolean())
                .put("xpack.security.audit.logfile.events.emit_request_body", includeRequestBody)
                .build();
        localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(buildNewFakeTransportAddress().toString());
        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        prefix = LoggingAuditTrail.resolvePrefix(settings, localNode);
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testAnonymousAccessDeniedTransport() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, clusterService.localNode(), threadContext);
        auditTrail.anonymousAccessDenied("_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [anonymous_access_denied]\t"  + origins +
                    ", action=[_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [anonymous_access_denied]\t"  + origins +
                    ", action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.anonymousAccessDenied("_action", message);
        assertEmptyLog(logger);
    }

    public void testAnonymousAccessDeniedRest() throws Exception {
        InetAddress address = forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1");
        Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", new InetSocketAddress(address, 9200));
        String expectedMessage = tuple.v1().expectedMessage();
        RestRequest request = tuple.v2();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.anonymousAccessDenied(request);
        if (includeRequestBody) {
            assertMsg(logger, Level.INFO, prefix + "[rest] [anonymous_access_denied]\torigin_address=[" +
                    NetworkAddress.format(address) + "], uri=[_uri], request_body=[" + expectedMessage + "]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[rest] [anonymous_access_denied]\torigin_address=[" +
                    NetworkAddress.format(address) + "], uri=[_uri]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.anonymousAccessDenied(request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailed() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);;
        auditTrail.authenticationFailed(new MockToken(), "_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [authentication_failed]\t" + origins +
                    ", principal=[_principal], action=[_action], indices=[" + indices(message) +
                    "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [authentication_failed]\t" + origins +
                    ", principal=[_principal], action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(new MockToken(), "_action", message);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);;
        auditTrail.authenticationFailed("_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [authentication_failed]\t" + origins +
                    ", action=[_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [authentication_failed]\t" + origins +
                    ", action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed("_action", message);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRest() throws Exception {
        InetAddress address = forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1");
        Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", new InetSocketAddress(address, 9200));
        String expectedMessage = tuple.v1().expectedMessage();
        RestRequest request = tuple.v2();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(new MockToken(), request);
        if (includeRequestBody) {
            assertMsg(logger, Level.INFO, prefix + "[rest] [authentication_failed]\torigin_address=[" +
                    NetworkAddress.format(address) + "], principal=[_principal], uri=[_uri], request_body=[" +
                    expectedMessage + "]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[rest] [authentication_failed]\torigin_address=[" +
                    NetworkAddress.format(address) + "], principal=[_principal], uri=[_uri]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(new MockToken(), request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        InetAddress address = forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1");
        Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", new InetSocketAddress(address, 9200));
        String expectedMessage = tuple.v1().expectedMessage();
        RestRequest request = tuple.v2();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(request);
        if (includeRequestBody) {
            assertMsg(logger, Level.INFO, prefix + "[rest] [authentication_failed]\torigin_address=[" +
                    NetworkAddress.format(address) + "], uri=[_uri], request_body=[" + expectedMessage + "]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[rest] [authentication_failed]\torigin_address=[" +
                    NetworkAddress.format(address) + "], uri=[_uri]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRealm() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        auditTrail.authenticationFailed("_realm", new MockToken(), "_action", message);
        assertEmptyLog(logger);

        // test enabled
        settings =
                Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "realm_authentication_failed").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed("_realm", new MockToken(), "_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [realm_authentication_failed]\trealm=[_realm], " + origins +
                    ", principal=[_principal], action=[_action], indices=[" + indices(message) + "], " +
                    "request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [realm_authentication_failed]\trealm=[_realm], " + origins +
                    ", principal=[_principal], action=[_action], request=[MockMessage]");
        }
    }

    public void testAuthenticationFailedRealmRest() throws Exception {
        InetAddress address = forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1");
        Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", new InetSocketAddress(address, 9200));
        String expectedMessage = tuple.v1().expectedMessage();
        RestRequest request = tuple.v2();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed("_realm", new MockToken(), request);
        assertEmptyLog(logger);

        // test enabled
        settings =
                Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "realm_authentication_failed").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed("_realm", new MockToken(), request);
        if (includeRequestBody) {
            assertMsg(logger, Level.INFO, prefix + "[rest] [realm_authentication_failed]\trealm=[_realm], origin_address=[" +
                    NetworkAddress.format(address) + "], principal=[_principal], uri=[_uri], request_body=[" +
                    expectedMessage + "]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[rest] [realm_authentication_failed]\trealm=[_realm], origin_address=[" +
                    NetworkAddress.format(address) + "], principal=[_principal], uri=[_uri]");
        }
    }

    public void testAccessGranted() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[]{"r1"},
                    new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        String userInfo = runAs ? "principal=[running as], run_by_principal=[_username]" : "principal=[_username]";
        auditTrail.accessGranted(user, "_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_granted]\t" + origins + ", " + userInfo +
                    ", action=[_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_granted]\t" + origins + ", " + userInfo +
                    ", action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "access_granted").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessGranted(user, "_action", message);
        assertEmptyLog(logger);
    }

    public void testAccessGrantedInternalSystemAction() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        auditTrail.accessGranted(SystemUser.INSTANCE, "internal:_action", message);
        assertEmptyLog(logger);

        // test enabled
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "system_access_granted").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessGranted(SystemUser.INSTANCE, "internal:_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_granted]\t" + origins + ", principal=[" +
                    SystemUser.INSTANCE.principal()
                    + "], action=[internal:_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_granted]\t" + origins + ", principal=[" +
                    SystemUser.INSTANCE.principal() + "], action=[internal:_action], request=[MockMessage]");
        }
    }

    public void testAccessGrantedInternalSystemActionNonSystemUser() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[]{"r1"},
                    new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        String userInfo = runAs ? "principal=[running as], run_by_principal=[_username]" : "principal=[_username]";
        auditTrail.accessGranted(user, "internal:_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_granted]\t" + origins + ", " + userInfo +
                    ", action=[internal:_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_granted]\t" + origins + ", " + userInfo +
                    ", action=[internal:_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "access_granted").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessGranted(user, "internal:_action", message);
        assertEmptyLog(logger);
    }

    public void testAccessDenied() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[]{"r1"},
                    new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        String userInfo = runAs ? "principal=[running as], run_by_principal=[_username]" : "principal=[_username]";
        auditTrail.accessDenied(user, "_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_denied]\t" + origins + ", " + userInfo +
                    ", action=[_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [access_denied]\t" + origins + ", " + userInfo +
                    ", action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "access_denied").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessDenied(user, "_action", message);
        assertEmptyLog(logger);
    }

    public void testTamperedRequestRest() throws Exception {
        InetAddress address = forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1");
        Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", new InetSocketAddress(address, 9200));
        String expectedMessage = tuple.v1().expectedMessage();
        RestRequest request = tuple.v2();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(request);
        if (includeRequestBody) {
            assertMsg(logger, Level.INFO, prefix + "[rest] [tampered_request]\torigin_address=[" +
                    NetworkAddress.format(address) + "], uri=[_uri], request_body=[" + expectedMessage + "]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[rest] [tampered_request]\torigin_address=[" +
                    NetworkAddress.format(address) + "], uri=[_uri]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "tampered_request").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(request);
        assertEmptyLog(logger);
    }

    public void testTamperedRequest() throws Exception {
        String action = "_action";
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(action, message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [tampered_request]\t" + origins +
                    ", action=[_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [tampered_request]\t" + origins +
                    ", action=[_action], request=[MockMessage]");
        }

        // test disabled

    }

    public void testTamperedRequestWithUser() throws Exception {
        String action = "_action";
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[]{"r1"}, new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        String userInfo = runAs ? "principal=[running as], run_by_principal=[_username]" : "principal=[_username]";
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(user, action, message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [tampered_request]\t" + origins + ", " + userInfo +
                    ", action=[_action], indices=[" + indices(message) + "], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [tampered_request]\t" + origins + ", " + userInfo +
                    ", action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "tampered_request").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(user, action, message);
        assertEmptyLog(logger);
    }

    public void testConnectionDenied() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        auditTrail.connectionDenied(inetAddress, "default", rule);
        assertMsg(logger, Level.INFO, String.format(Locale.ROOT, prefix +
                        "[ip_filter] [connection_denied]\torigin_address=[%s], transport_profile=[%s], rule=[deny %s]",
                NetworkAddress.format(inetAddress), "default", "_all"));

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "connection_denied").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.connectionDenied(inetAddress, "default", rule);
        assertEmptyLog(logger);
    }

    public void testConnectionGranted() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        auditTrail.connectionGranted(inetAddress, "default", rule);
        assertEmptyLog(logger);

        // test enabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "connection_granted").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.connectionGranted(inetAddress, "default", rule);
        assertMsg(logger, Level.INFO, String.format(Locale.ROOT, prefix + "[ip_filter] [connection_granted]\torigin_address=[%s], " +
                "transport_profile=[default], rule=[allow default:accept_all]", NetworkAddress.format(inetAddress)));
    }

    public void testRunAsGranted() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = new MockMessage(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        User user = new User("_username", new String[]{"r1"}, new User("running as", new String[] {"r2"}));
        auditTrail.runAsGranted(user, "_action", message);
        assertMsg(logger, Level.INFO, prefix + "[transport] [run_as_granted]\t" + origins +
                ", principal=[_username], run_as_principal=[running as], action=[_action], request=[MockMessage]");

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "run_as_granted").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.runAsGranted(user, "_action", message);
        assertEmptyLog(logger);
    }

    public void testRunAsDenied() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = new MockMessage(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        User user = new User("_username", new String[]{"r1"}, new User("running as", new String[] {"r2"}));
        auditTrail.runAsDenied(user, "_action", message);
        assertMsg(logger, Level.INFO, prefix + "[transport] [run_as_denied]\t" + origins +
                ", principal=[_username], run_as_principal=[running as], action=[_action], request=[MockMessage]");

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "run_as_denied").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.runAsDenied(user, "_action", message);
        assertEmptyLog(logger);
    }

    public void testOriginAttributes() throws Exception {

        MockMessage message = new MockMessage(threadContext);
        String text = LoggingAuditTrail.originAttributes(message, localNode, threadContext);;
        InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress != null) {
            assertThat(text, equalTo("origin_type=[rest], origin_address=[" +
                    NetworkAddress.format(restAddress.getAddress()) + "]"));
            return;
        }
        TransportAddress address = message.remoteAddress();
        if (address == null) {
            assertThat(text, equalTo("origin_type=[local_node], origin_address=[" + localNode.getHostAddress() + "]"));
            return;
        }

        assertThat(text, equalTo("origin_type=[transport], origin_address=[" +
                    NetworkAddress.format(address.address().getAddress()) + "]"));
    }

    public void testAuthenticationSuccessRest() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("foo", "bar");
        InetAddress address = forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1");
        Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", new InetSocketAddress(address, 9200), params);
        String expectedMessage = tuple.v1().expectedMessage();
        RestRequest request = tuple.v2();
        boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[] { "r1" }, new User("running as", new String[] { "r2" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        String userInfo = runAs ? "principal=[running as], run_by_principal=[_username]" : "principal=[_username]";
        String realm = "_realm";

        Settings settings = Settings.builder().put(this.settings)
                .put("xpack.security.audit.logfile.events.include", "authentication_success")
                .build();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationSuccess(realm, user, request);
        if (includeRequestBody) {
            assertMsg(logger, Level.INFO,
                    prefix + "[rest] [authentication_success]\t" + userInfo + ", realm=[_realm], uri=[_uri], params=[" + params
                    + "], request_body=[" + expectedMessage + "]");
        } else {
            assertMsg(logger, Level.INFO,
                    prefix + "[rest] [authentication_success]\t" + userInfo + ", realm=[_realm], uri=[_uri], params=[" + params + "]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder().put(this.settings).put("xpack.security.audit.logfile.events.exclude", "authentication_success")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationSuccess(realm, user, request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        Settings settings = Settings.builder().put(this.settings)
                .put("xpack.security.audit.logfile.events.include", "authentication_success").build();
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO);
        LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        String origins = LoggingAuditTrail.originAttributes(message, localNode, threadContext);
        boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[] { "r1" }, new User("running as", new String[] { "r2" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        String userInfo = runAs ? "principal=[running as], run_by_principal=[_username]" : "principal=[_username]";
        String realm = "_realm";
        auditTrail.authenticationSuccess(realm, user, "_action", message);
        if (message instanceof IndicesRequest) {
            assertMsg(logger, Level.INFO, prefix + "[transport] [authentication_success]\t" + origins + ", " + userInfo
                    + ", realm=[_realm], action=[_action], request=[MockIndicesRequest]");
        } else {
            assertMsg(logger, Level.INFO, prefix + "[transport] [authentication_success]\t" + origins + ", " + userInfo
                    + ", realm=[_realm], action=[_action], request=[MockMessage]");
        }

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(this.settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_success")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationSuccess(realm, user, "_action", message);
        assertEmptyLog(logger);
    }

    private void assertMsg(Logger logger, Level level, String message) {
        List<String> output = CapturingLogger.output(logger.getName(), level);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), equalTo(message));
    }

    private void assertEmptyLog(Logger logger) {
        assertThat(CapturingLogger.isEmpty(logger.getName()), is(true));
    }

    private Tuple<RestContent, RestRequest> prepareRestContent(String uri, InetSocketAddress remoteAddress) {
        return prepareRestContent(uri, remoteAddress, Collections.emptyMap());
    }

    private Tuple<RestContent, RestRequest> prepareRestContent(String uri, InetSocketAddress remoteAddress, Map<String, String> params) {
        RestContent content = randomFrom(RestContent.values());
        FakeRestRequest.Builder builder = new Builder(NamedXContentRegistry.EMPTY);
        if (content.hasContent()) {
            builder.withContent(content.content(), XContentType.JSON);
        }
        builder.withPath(uri);
        builder.withRemoteAddress(remoteAddress);
        builder.withParams(params);
        return new Tuple<>(content, builder.build());
    }

    /** creates address without any lookups. hostname can be null, for missing */
    private static InetAddress forge(String hostname, String address) throws IOException {
        byte bytes[] = InetAddress.getByName(address).getAddress();
        return InetAddress.getByAddress(hostname, bytes);
    }

    private static String indices(TransportMessage message) {
        return Strings.collectionToCommaDelimitedString(AuditUtil.indices(message));
    }

    private static class MockMessage extends TransportMessage {

        private MockMessage(ThreadContext threadContext) throws IOException {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    remoteAddress(buildNewFakeTransportAddress());
                } else {
                    remoteAddress(new TransportAddress(InetAddress.getLoopbackAddress(), 1234));
                }
            }
            if (randomBoolean()) {
                RemoteHostHeader.putRestRemoteAddress(threadContext, new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234));
            }
        }
    }

    private static class MockIndicesRequest extends org.elasticsearch.action.MockIndicesRequest {

        private MockIndicesRequest(ThreadContext threadContext) throws IOException {
            super(IndicesOptions.strictExpandOpenAndForbidClosed(), "idx1", "idx2");
            if (randomBoolean()) {
                remoteAddress(buildNewFakeTransportAddress());
            }
            if (randomBoolean()) {
                RemoteHostHeader.putRestRemoteAddress(threadContext, new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234));
            }
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
