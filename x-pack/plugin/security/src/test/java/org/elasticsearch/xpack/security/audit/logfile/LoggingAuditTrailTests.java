/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.FakeRestRequest.Builder;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoggingAuditTrailTests extends ESTestCase {

    enum RestContent {
        VALID() {
            @Override
            protected boolean hasContent() {
                return true;
            }

            @Override
            protected BytesReference content() {
                return new BytesArray("{ \"key\": \"value\" }");
            }

            @Override
            protected String expectedMessage() {
                return "{ \"key\": \"value\" }";
            }
        },
        INVALID() {
            @Override
            protected boolean hasContent() {
                return true;
            }

            @Override
            protected BytesReference content() {
                return new BytesArray("{ \"key\": \"value\" ");
            }

            @Override
            protected String expectedMessage() {
                return "{ \"key\": \"value\" ";
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

    private static PatternLayout patternLayout;
    private Settings settings;
    private DiscoveryNode localNode;
    private ClusterService clusterService;
    private ThreadContext threadContext;
    private boolean includeRequestBody;
    private Map<String, String> commonFields;
    private Logger logger;
    private LoggingAuditTrail auditTrail;

    @BeforeClass
    public static void lookupPatternLayout() throws Exception {
        final Properties properties = new Properties();
        try (InputStream configStream = LoggingAuditTrail.class.getClassLoader().getResourceAsStream("log4j2.properties")) {
            properties.load(configStream);
        }
        // This is a minimal and brittle parsing of the security log4j2 config
        // properties. If any of these fails, then surely the config file changed. In
        // this case adjust the assertions! The goal of this assertion chain is to
        // validate that the layout pattern we are testing with is indeed the one
        // attached to the LoggingAuditTrail.class logger.
        assertThat(properties.getProperty("logger.xpack_security_audit_logfile.name"), is(LoggingAuditTrail.class.getName()));
        assertThat(properties.getProperty("logger.xpack_security_audit_logfile.appenderRef.audit_rolling.ref"), is("audit_rolling"));
        assertThat(properties.getProperty("appender.audit_rolling.name"), is("audit_rolling"));
        assertThat(properties.getProperty("appender.audit_rolling.layout.type"), is("PatternLayout"));
        final String patternLayoutFormat = properties.getProperty("appender.audit_rolling.layout.pattern");
        assertThat(patternLayoutFormat, is(notNullValue()));
        patternLayout = PatternLayout.newBuilder().withPattern(patternLayoutFormat).withCharset(StandardCharsets.UTF_8).build();
    }

    @AfterClass
    public static void releasePatternLayout() {
        patternLayout = null;
    }

    @Before
    public void init() throws Exception {
        includeRequestBody = randomBoolean();
        settings = Settings.builder()
                .put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_NODE_ID_SETTING.getKey(), randomBoolean())
                .put("xpack.security.audit.logfile.events.emit_request_body", includeRequestBody)
                .build();
        localNode = mock(DiscoveryNode.class);
        when(localNode.getId()).thenReturn(randomAlphaOfLength(16));
        when(localNode.getAddress()).thenReturn(buildNewFakeTransportAddress());
        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        Mockito.doAnswer((Answer) invocation -> {
            final LoggingAuditTrail arg0 = (LoggingAuditTrail) invocation.getArguments()[0];
            arg0.updateLocalNodeInfo(localNode);
            return null;
        }).when(clusterService).addListener(Mockito.isA(LoggingAuditTrail.class));
        final ClusterSettings clusterSettings = mockClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        commonFields = new LoggingAuditTrail.EntryCommonFields(settings, localNode).commonFields;
        threadContext = new ThreadContext(Settings.EMPTY);
        if (randomBoolean()) {
            threadContext.putHeader(Task.X_OPAQUE_ID, randomAlphaOfLengthBetween(1, 4));
        }
        if (randomBoolean()) {
            threadContext.putHeader(AuditTrail.X_FORWARDED_FOR_HEADER,
                    randomFrom("2001:db8:85a3:8d3:1319:8a2e:370:7348", "203.0.113.195", "203.0.113.195, 70.41.3.18, 150.172.238.178"));
        }
        logger = CapturingLogger.newCapturingLogger(Level.INFO, patternLayout);
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
    }

    @After
    public void clearLog() throws Exception {
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
    }

    public void testAnonymousAccessDeniedTransport() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.anonymousAccessDenied(requestId, "_action", message);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        indicesRequest(message, checkedFields, checkedArrayFields);
        restOrTransportOrigin(message, threadContext, checkedFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.anonymousAccessDenied(requestId, "_action", message);
        assertEmptyLog(logger);
    }

    public void testAnonymousAccessDeniedRest() throws Exception {
        final InetSocketAddress address = new InetSocketAddress(forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
                randomIntBetween(9200, 9300));
        final Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", address);
        final String expectedMessage = tuple.v1().expectedMessage();
        final RestRequest request = tuple.v2();

        final String requestId = randomRequestId();
        auditTrail.anonymousAccessDenied(requestId, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
                .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(expectedMessage)) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, expectedMessage);
        }
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.anonymousAccessDenied(requestId, request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailed() throws Exception {
        final AuthenticationToken mockToken = new MockToken();
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, mockToken, "_action", message);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, mockToken.principal())
                     .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(requestId, new MockToken(), "_action", message);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, "_action", message);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                     .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(requestId, "_action", message);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("foo", "bar");
        }
        final InetSocketAddress address = new InetSocketAddress(forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
                randomIntBetween(9200, 9300));
        final Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final String expectedMessage = tuple.v1().expectedMessage();
        final RestRequest request = tuple.v2();
        final AuthenticationToken mockToken = new MockToken();

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, mockToken, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, mockToken.principal())
                     .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                     .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                     .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(expectedMessage)) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, expectedMessage);
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "foo=bar");
        }
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(requestId, new MockToken(), request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("bar", "baz");
        }
        final InetSocketAddress address = new InetSocketAddress(forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
                randomIntBetween(9200, 9300));
        final Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final String expectedMessage = tuple.v1().expectedMessage();
        final RestRequest request = tuple.v2();

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                     .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                     .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(expectedMessage)) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, expectedMessage);
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "bar=baz");
        }
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(requestId, request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRealm() throws Exception {
        final AuthenticationToken mockToken = new MockToken();
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String realm = randomAlphaOfLengthBetween(1, 6);
        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, realm, mockToken, "_action", message);
        assertEmptyLog(logger);

        // test enabled
        settings = Settings.builder()
                       .put(settings)
                       .put("xpack.security.audit.logfile.events.include", "realm_authentication_failed")
                       .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(requestId, realm, mockToken, "_action", message);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                     .put(LoggingAuditTrail.REALM_FIELD_NAME, realm)
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, mockToken.principal())
                     .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                     .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());
    }

    public void testAuthenticationFailedRealmRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("_param", "baz");
        }
        final InetSocketAddress address = new InetSocketAddress(forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
                randomIntBetween(9200, 9300));
        final Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final String expectedMessage = tuple.v1().expectedMessage();
        final RestRequest request = tuple.v2();
        final AuthenticationToken mockToken = new MockToken();
        final String realm = randomAlphaOfLengthBetween(1, 6);
        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, realm, mockToken, request);
        assertEmptyLog(logger);

        // test enabled
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "realm_authentication_failed")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationFailed(requestId, realm, mockToken, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                     .put(LoggingAuditTrail.REALM_FIELD_NAME, realm)
                     .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, mockToken.principal())
                     .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                     .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(expectedMessage)) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, expectedMessage);
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "_param=baz");
        }
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());
    }

    public void testAccessGranted() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        final String requestId = randomRequestId();

        auditTrail.accessGranted(requestId, authentication, "_action", message, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        subject(authentication, checkedFields);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "access_granted")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessGranted(requestId, authentication, "_action", message, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testAccessGrantedInternalSystemAction() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = new Authentication(SystemUser.INSTANCE, new RealmRef("_reserved", "test", "foo"), null);
        final String requestId = randomRequestId();
        auditTrail.accessGranted(requestId, authentication, "internal:_action", message, authorizationInfo);
        assertEmptyLog(logger);

        // test enabled
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "system_access_granted")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessGranted(requestId, authentication, "internal:_action", message, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, SystemUser.INSTANCE.principal())
                .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, "_reserved")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "internal:_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());
    }

    public void testAccessGrantedInternalSystemActionNonSystemUser() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        final String requestId = randomRequestId();

        auditTrail.accessGranted(requestId, authentication, "internal:_action", message, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "internal:_action")
            .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        subject(authentication, checkedFields);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                    .put(settings)
                    .put("xpack.security.audit.logfile.events.exclude", "access_granted")
                    .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessGranted(requestId, authentication, "internal:_action", message, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testAccessDenied() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        final String requestId = randomRequestId();

        auditTrail.accessDenied(requestId, authentication, "_action/bar", message, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_denied")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action/bar")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        subject(authentication, checkedFields);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);

        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "access_denied")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.accessDenied(requestId, authentication, "_action", message, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testTamperedRequestRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("_param", "baz");
        }
        final InetSocketAddress address = new InetSocketAddress(forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
                randomIntBetween(9200, 9300));
        final Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final String expectedMessage = tuple.v1().expectedMessage();
        final RestRequest request = tuple.v2();
        final String requestId = randomRequestId();
        auditTrail.tamperedRequest(requestId, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
                .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(expectedMessage)) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, expectedMessage);
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "_param=baz");
        }
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "tampered_request")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(requestId, request);
        assertEmptyLog(logger);
    }

    public void testTamperedRequest() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.tamperedRequest(requestId, "_action", message);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "tampered_request")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(requestId, "_action", message);
        assertEmptyLog(logger);
    }

    public void testTamperedRequestWithUser() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final boolean runAs = randomBoolean();
        final User user;
        if (runAs) {
            user = new User("running_as", new String[] { "r2" }, new User("_username", new String[] { "r1" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }

        final String requestId = randomRequestId();
        auditTrail.tamperedRequest(requestId, user, "_action", message);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        if (runAs) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "running_as");
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME, "_username");
        } else {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username");
        }
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "tampered_request")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.tamperedRequest(requestId, user, "_action", message);
        assertEmptyLog(logger);
    }

    public void testConnectionDenied() throws Exception {
        final InetAddress inetAddress = InetAddress.getLoopbackAddress();
        final SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        final String profile = randomBoolean() ? IPFilter.HTTP_PROFILE_NAME : randomAlphaOfLengthBetween(1, 6);

        auditTrail.connectionDenied(inetAddress, profile, rule);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.IP_FILTER_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "connection_denied")
                .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME,
                        IPFilter.HTTP_PROFILE_NAME.equals(profile) ? LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE
                                : LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
                .put(LoggingAuditTrail.TRANSPORT_PROFILE_FIELD_NAME, profile)
                .put(LoggingAuditTrail.RULE_FIELD_NAME, "deny _all");
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "connection_denied")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.connectionDenied(inetAddress, profile, rule);
        assertEmptyLog(logger);
    }

    public void testConnectionGranted() throws Exception {
        final InetAddress inetAddress = InetAddress.getLoopbackAddress();
        final SecurityIpFilterRule rule = IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        final String profile = randomBoolean() ? IPFilter.HTTP_PROFILE_NAME : randomAlphaOfLengthBetween(1, 6);

        auditTrail.connectionGranted(inetAddress, profile, rule);
        assertEmptyLog(logger);

        // test enabled
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "connection_granted")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.connectionGranted(inetAddress, profile, rule);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.IP_FILTER_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "connection_granted")
                .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME,
                        IPFilter.HTTP_PROFILE_NAME.equals(profile) ? LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE
                                : LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
                .put(LoggingAuditTrail.TRANSPORT_PROFILE_FIELD_NAME, profile)
                .put(LoggingAuditTrail.RULE_FIELD_NAME, "allow default:accept_all");
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());
    }

    public void testRunAsGranted() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = new Authentication(
                new User("running as", new String[] { "r2" }, new User("_username", new String[] { "r1" })),
                new RealmRef("authRealm", "test", "foo"),
                new RealmRef("lookRealm", "up", "by"));
        final String requestId = randomRequestId();

        auditTrail.runAsGranted(requestId, authentication, "_action", message, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "run_as_granted")
                .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username")
                .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, "authRealm")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME, "running as")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME, "lookRealm")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "run_as_granted")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.runAsGranted(requestId, authentication, "_action", message, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testRunAsDenied() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = new Authentication(
                new User("running as", new String[] { "r2" }, new User("_username", new String[] { "r1" })),
                new RealmRef("authRealm", "test", "foo"),
                new RealmRef("lookRealm", "up", "by"));
        final String requestId = randomRequestId();

        auditTrail.runAsDenied(requestId, authentication, "_action", message, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "run_as_denied")
                .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username")
                .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, "authRealm")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME, "running as")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME, "lookRealm")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "run_as_denied")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.runAsDenied(requestId, authentication, "_action", message, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testAuthenticationSuccessRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("foo", "bar");
            params.put("evac", "true");
        }
        final InetSocketAddress address = new InetSocketAddress(forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
                randomIntBetween(9200, 9300));
        final Tuple<RestContent, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final String expectedMessage = tuple.v1().expectedMessage();
        final RestRequest request = tuple.v2();
        final String realm = randomAlphaOfLengthBetween(1, 6);
        final User user;
        if (randomBoolean()) {
            user = new User("running as", new String[] { "r2" }, new User("_username", new String[] { "r1" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        final String requestId = randomRequestId();

        // event by default disabled
        auditTrail.authenticationSuccess(requestId, realm, user, request);
        assertEmptyLog(logger);

        settings = Settings.builder()
                .put(this.settings)
                .put("xpack.security.audit.logfile.events.include", "authentication_success")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationSuccess(requestId, realm, user, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
                     .put(LoggingAuditTrail.REALM_FIELD_NAME, realm)
                     .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                     .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                     .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(expectedMessage)) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, expectedMessage);
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "foo=bar&evac=true");
        }
        if (user.isRunAs()) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "running as");
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME, "_username");
        } else {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username");
        }
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        final TransportMessage message = randomBoolean() ? new MockMessage(threadContext) : new MockIndicesRequest(threadContext);
        final User user;
        if (randomBoolean()) {
            user = new User("running as", new String[] { "r2" }, new User("_username", new String[] { "r1" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        final String realm = randomAlphaOfLengthBetween(1, 6);
        final String requestId = randomRequestId();

        // event by default disabled
        auditTrail.authenticationSuccess(requestId, realm, user, "_action", message);
        assertEmptyLog(logger);

        settings = Settings.builder()
                .put(this.settings)
                .put("xpack.security.audit.logfile.events.include", "authentication_success")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        auditTrail.authenticationSuccess(requestId, realm, user, "_action", message);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REALM_FIELD_NAME, realm)
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, message.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        if (user.isRunAs()) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "running as");
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME, "_username");
        } else {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username");
        }
        restOrTransportOrigin(message, threadContext, checkedFields);
        indicesRequest(message, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());
    }

    public void testRequestsWithoutIndices() throws Exception {
        settings = Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "_all")
                .build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        final User user = new User("_username", new String[] { "r1" });
        final AuthorizationInfo authorizationInfo =
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, new String[] { randomAlphaOfLengthBetween(1, 6) });
        final String realm = randomAlphaOfLengthBetween(1, 6);
        // transport messages without indices
        final TransportMessage[] messages = new TransportMessage[] { new MockMessage(threadContext),
                new org.elasticsearch.action.MockIndicesRequest(IndicesOptions.strictExpandOpenAndForbidClosed(), new String[0]),
                new org.elasticsearch.action.MockIndicesRequest(IndicesOptions.strictExpandOpenAndForbidClosed(), (String[]) null) };
        final List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        int logEntriesCount = 1;
        for (final TransportMessage message : messages) {
            auditTrail.anonymousAccessDenied("_req_id", "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationFailed("_req_id", new MockToken(), "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationFailed("_req_id", "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationFailed("_req_id", realm, new MockToken(), "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.accessGranted("_req_id", createAuthentication(), "_action", message, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.accessDenied("_req_id", createAuthentication(), "_action", message, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.tamperedRequest("_req_id", "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.tamperedRequest("_req_id", user, "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.runAsGranted("_req_id", createAuthentication(), "_action", message, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.runAsDenied("_req_id", createAuthentication(), "_action", message, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationSuccess("_req_id", realm, user, "_action", message);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
        }
    }

    private void assertMsg(Logger logger, Map<String, String> checkFields) {
        assertMsg(logger, checkFields, Collections.emptyMap());
    }

    private void assertMsg(Logger logger, Map<String, String> checkFields, Map<String, String[]> checkArrayFields) {
        final List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat("Exactly one logEntry expected. Found: " + output.size(), output.size(), is(1));
        if (checkFields == null) {
            // only check msg existence
            return;
        }
        String logLine = output.get(0);
        // check each field
        for (final Map.Entry<String, String> checkField : checkFields.entrySet()) {
            if (null == checkField.getValue()) {
                // null checkField means that the field does not exist
                assertThat("Field: " + checkField.getKey() + " should be missing.",
                        logLine.contains(Pattern.quote("\"" + checkField.getKey() + "\":")), is(false));
            } else {
                final String quotedValue = "\"" + checkField.getValue().replaceAll("\"", "\\\\\"") + "\"";
                final Pattern logEntryFieldPattern = Pattern.compile(Pattern.quote("\"" + checkField.getKey() + "\":" + quotedValue));
                assertThat("Field " + checkField.getKey() + " value mismatch. Expected " + quotedValue,
                        logEntryFieldPattern.matcher(logLine).find(), is(true));
                // remove checked field
                logLine = logEntryFieldPattern.matcher(logLine).replaceFirst("");
            }
        }
        for (final Map.Entry<String, String[]> checkArrayField : checkArrayFields.entrySet()) {
            if (null == checkArrayField.getValue()) {
                // null checkField means that the field does not exist
                assertThat("Field: " + checkArrayField.getKey() + " should be missing.",
                        logLine.contains(Pattern.quote("\"" + checkArrayField.getKey() + "\":")), is(false));
            } else {
                final String quotedValue = "[" + Arrays.asList(checkArrayField.getValue())
                        .stream()
                        .filter(s -> s != null)
                        .map(s -> "\"" + s.replaceAll("\"", "\\\\\"") + "\"")
                        .reduce((x, y) -> x + "," + y)
                        .orElse("") + "]";
                final Pattern logEntryFieldPattern = Pattern.compile(Pattern.quote("\"" + checkArrayField.getKey() + "\":" + quotedValue));
                assertThat("Field " + checkArrayField.getKey() + " value mismatch. Expected " + quotedValue + ".\nLog line: " + logLine,
                        logEntryFieldPattern.matcher(logLine).find(), is(true));
                // remove checked field
                logLine = logEntryFieldPattern.matcher(logLine).replaceFirst("");
            }
        }
        logLine = logLine.replaceFirst("\"" + LoggingAuditTrail.LOG_TYPE + "\":\"audit\", ", "")
                         .replaceFirst("\"" + LoggingAuditTrail.TIMESTAMP + "\":\"[^\"]*\"", "")
                         .replaceAll("[{},]", "");
        // check no extra fields
        assertThat("Log event has extra unexpected content: " + logLine, Strings.hasText(logLine), is(false));
    }

    private void assertEmptyLog(Logger logger) {
        assertThat("Logger is not empty", CapturingLogger.isEmpty(logger.getName()), is(true));
    }

    protected Tuple<RestContent, RestRequest> prepareRestContent(String uri, InetSocketAddress remoteAddress) {
        return prepareRestContent(uri, remoteAddress, Collections.emptyMap());
    }

    private Tuple<RestContent, RestRequest> prepareRestContent(String uri, InetSocketAddress remoteAddress, Map<String, String> params) {
        final RestContent content = randomFrom(RestContent.values());
        final FakeRestRequest.Builder builder = new Builder(NamedXContentRegistry.EMPTY);
        if (content.hasContent()) {
            builder.withContent(content.content(), XContentType.JSON);
        }
        if (params.isEmpty()) {
            builder.withPath(uri);
        } else {
            final StringBuilder queryString = new StringBuilder("?");
            for (final Map.Entry<String, String> entry : params.entrySet()) {
                if (queryString.length() > 1) {
                    queryString.append('&');
                }
                queryString.append(entry.getKey() + "=" + entry.getValue());
            }
            builder.withPath(uri + queryString.toString());
        }
        builder.withRemoteAddress(remoteAddress);
        builder.withParams(params);
        builder.withMethod(randomFrom(RestRequest.Method.values()));
        return new Tuple<>(content, builder.build());
    }

    /** creates address without any lookups. hostname can be null, for missing */
    protected static InetAddress forge(String hostname, String address) throws IOException {
        final byte bytes[] = InetAddress.getByName(address).getAddress();
        return InetAddress.getByAddress(hostname, bytes);
    }

    private static Authentication createAuthentication() {
        final RealmRef lookedUpBy;
        final User user;
        if (randomBoolean()) {
            user = new User("running_as", new String[] { "r2" }, new User("_username", new String[] { "r1" }));
            lookedUpBy = new RealmRef("lookRealm", "up", "by");
        } else {
            user = new User("_username", new String[] { "r1" });
            lookedUpBy = null;
        }
        return new Authentication(user, new RealmRef("authRealm", "test", "foo"), lookedUpBy);
    }

    private ClusterSettings mockClusterSettings() {
        final List<Setting<?>> settingsList = new ArrayList<>();
        LoggingAuditTrail.registerSettings(settingsList);
        settingsList.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ClusterSettings(settings, new HashSet<>(settingsList));
    }

    static class MockMessage extends TransportMessage {

        MockMessage(ThreadContext threadContext) throws IOException {
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    static class MockIndicesRequest extends org.elasticsearch.action.MockIndicesRequest {

        MockIndicesRequest(ThreadContext threadContext) throws IOException {
            super(IndicesOptions.strictExpandOpenAndForbidClosed(),
                    randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4)));
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

    /**
     * @return A tuple of ( id-to-pass-to-audit-trail, id-to-check-in-audit-message )
     */
    private String randomRequestId() {
        return randomBoolean() ? randomAlphaOfLengthBetween(8, 24) : AuditUtil.generateRequestId(threadContext);
    }

    private static void restOrTransportOrigin(TransportMessage message, ThreadContext threadContext,
                                              MapBuilder<String, String> checkedFields) {
        final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress != null) {
            checkedFields.put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                    .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
        } else {
            final TransportAddress address = message.remoteAddress();
            if (address != null) {
                checkedFields.put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                        .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address.address()));
            }
        }
    }

    private static void subject(Authentication authentication, MapBuilder<String, String> checkedFields) {
        checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authentication.getUser().principal());
        if (authentication.getUser().isRunAs()) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authentication.getLookedUpBy().getName())
                         .put(LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME, authentication.getUser().authenticatedUser().principal())
                         .put(LoggingAuditTrail.PRINCIPAL_RUN_BY_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
        } else {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
        }
    }

    private static void opaqueId(ThreadContext threadContext, MapBuilder<String, String> checkedFields) {
        final String opaqueId = threadContext.getHeader(Task.X_OPAQUE_ID);
        if (opaqueId != null) {
            checkedFields.put(LoggingAuditTrail.OPAQUE_ID_FIELD_NAME, opaqueId);
        }
    }

    private static void forwardedFor(ThreadContext threadContext ,MapBuilder<String, String> checkedFields) {
        final String forwardedFor = threadContext.getHeader(AuditTrail.X_FORWARDED_FOR_HEADER);
        if (forwardedFor != null) {
            checkedFields.put(LoggingAuditTrail.X_FORWARDED_FOR_FIELD_NAME, forwardedFor);
        }
    }

    private static void indicesRequest(TransportMessage message, MapBuilder<String, String> checkedFields,
                                       MapBuilder<String, String[]> checkedArrayFields) {
        if (message instanceof IndicesRequest) {
            checkedFields.put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, MockIndicesRequest.class.getSimpleName());
            checkedArrayFields.put(LoggingAuditTrail.INDICES_FIELD_NAME, ((IndicesRequest) message).indices());
        } else {
            checkedFields.put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, MockMessage.class.getSimpleName());
        }
    }

}
