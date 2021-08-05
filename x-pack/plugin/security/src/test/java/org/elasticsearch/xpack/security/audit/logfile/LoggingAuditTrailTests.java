/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.FakeRestRequest.Builder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequest;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.security.rest.RemoteHostHeader;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.stubbing.Answer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_NAME_FIELD;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_SOURCE_FIELD;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.authc.ApiKeyServiceTests.Utils.createApiKeyAuthentication;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
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
    private static String customAnonymousUsername;
    private static boolean reservedRealmEnabled;
    private Settings settings;
    private DiscoveryNode localNode;
    private ClusterService clusterService;
    private ThreadContext threadContext;
    private boolean includeRequestBody;
    private Map<String, String> commonFields;
    private Logger logger;
    private LoggingAuditTrail auditTrail;
    private ApiKeyService apiKeyService;

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
        customAnonymousUsername = randomAlphaOfLength(8);
        reservedRealmEnabled = randomBoolean();
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
                .put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), includeRequestBody)
                .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), reservedRealmEnabled)
                .put(AnonymousUser.USERNAME_SETTING.getKey(), customAnonymousUsername)
                .putList(AnonymousUser.ROLES_SETTING.getKey(), randomFrom(List.of(), List.of("smth")))
                .build();
        localNode = mock(DiscoveryNode.class);
        when(localNode.getId()).thenReturn(randomAlphaOfLength(16));
        when(localNode.getAddress()).thenReturn(buildNewFakeTransportAddress());
        Client client = mock(Client.class);
        SecurityIndexManager securityIndexManager = mock(SecurityIndexManager.class);
        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        Mockito.doAnswer((Answer) invocation -> {
            final LoggingAuditTrail arg0 = (LoggingAuditTrail) invocation.getArguments()[0];
            arg0.updateLocalNodeInfo(localNode);
            return null;
        }).when(clusterService).addListener(Mockito.isA(LoggingAuditTrail.class));
        final ClusterSettings clusterSettings = new ClusterSettings(settings,
                Set.of(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING, LoggingAuditTrail.EMIT_HOST_NAME_SETTING,
                        LoggingAuditTrail.EMIT_NODE_NAME_SETTING, LoggingAuditTrail.EMIT_NODE_ID_SETTING,
                        LoggingAuditTrail.INCLUDE_EVENT_SETTINGS, LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS,
                        LoggingAuditTrail.INCLUDE_REQUEST_BODY, LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS,
                        LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS, LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES,
                        LoggingAuditTrail.FILTER_POLICY_IGNORE_INDICES, LoggingAuditTrail.FILTER_POLICY_IGNORE_ACTIONS,
                        Loggers.LOG_LEVEL_SETTING));
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
        logger = CapturingLogger.newCapturingLogger(randomFrom(Level.OFF, Level.FATAL, Level.ERROR, Level.WARN, Level.INFO), patternLayout);
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        apiKeyService = new ApiKeyService(settings, Clock.systemUTC(), client, new XPackLicenseState(settings, () -> 0),
                                          securityIndexManager, clusterService,
                                          mock(CacheInvalidatorRegistry.class), mock(ThreadPool.class));
    }

    @After
    public void clearLog() throws Exception {
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
    }

    public void testEventsSettingValidation() {
        final String prefix = "xpack.security.audit.logfile.events.";
        Settings settings = Settings.builder().putList(prefix + "include", Arrays.asList("access_granted", "bogus")).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.get(settings));
        assertThat(e, hasToString(containsString("invalid event name specified [bogus]")));

        Settings settings2 = Settings.builder().putList(prefix + "exclude", Arrays.asList("access_denied", "foo")).build();
        e = expectThrows(IllegalArgumentException.class, () -> LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS.get(settings2));
        assertThat(e, hasToString(containsString("invalid event name specified [foo]")));
    }

    public void testAuditFilterSettingValidation() {
        final String prefix = "xpack.security.audit.logfile.events.";
        Settings settings =
                Settings.builder().putList(prefix + "ignore_filters.filter1.users", Arrays.asList("mickey", "/bogus")).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace("filter1").get(settings));
        assertThat(e, hasToString(containsString("invalid pattern [/bogus]")));

        Settings settings2 = Settings.builder()
                .putList(prefix + "ignore_filters.filter2.users", Arrays.asList("tom", "cruise"))
                .putList(prefix + "ignore_filters.filter2.realms", Arrays.asList("native", "/foo")).build();
        assertThat(LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace("filter2").get(settings2),
                containsInAnyOrder("tom", "cruise"));
        e = expectThrows(IllegalArgumentException.class,
                () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace("filter2").get(settings2));
        assertThat(e, hasToString(containsString("invalid pattern [/foo]")));

        Settings settings3 = Settings.builder()
                .putList(prefix + "ignore_filters.filter3.realms", Arrays.asList("native", "oidc1"))
                .putList(prefix + "ignore_filters.filter3.roles", Arrays.asList("kibana", "/wrong")).build();
        assertThat(LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace("filter3").get(settings3),
                containsInAnyOrder("native", "oidc1"));
        e = expectThrows(IllegalArgumentException.class,
                () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace("filter3").get(settings3));
        assertThat(e, hasToString(containsString("invalid pattern [/wrong]")));

        Settings settings4 = Settings.builder()
                .putList(prefix + "ignore_filters.filter4.roles", Arrays.asList("kibana", "elastic"))
                .putList(prefix + "ignore_filters.filter4.indices", Arrays.asList("index-1", "/no-inspiration")).build();
        assertThat(LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace("filter4").get(settings4),
                containsInAnyOrder("kibana", "elastic"));
        e = expectThrows(IllegalArgumentException.class,
                () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_INDICES.getConcreteSettingForNamespace("filter4").get(settings4));
        assertThat(e, hasToString(containsString("invalid pattern [/no-inspiration]")));

        Settings settings5 = Settings.builder()
            .putList(prefix + "ignore_filters.filter2.users", Arrays.asList("tom", "cruise"))
            .putList(prefix + "ignore_filters.filter2.actions", Arrays.asList("indices:data/read/*", "/foo")).build();
        assertThat(LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace("filter2").get(settings5),
            containsInAnyOrder("tom", "cruise"));
        e = expectThrows(IllegalArgumentException.class,
            () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_ACTIONS.getConcreteSettingForNamespace("filter2").get(settings5));
        assertThat(e, hasToString(containsString("invalid pattern [/foo]")));
    }

    public void testSecurityConfigChangeEventFormattingForRoles() throws IOException {
        final Path path = getDataPath("/org/elasticsearch/xpack/security/audit/logfile/audited_roles.txt");
        final Map<String, String> auditedRolesMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(Files.newInputStream(path)),
                StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // even number of lines
                auditedRolesMap.put(line, reader.readLine());
            }
        }

        RoleDescriptor nullRoleDescriptor = new RoleDescriptor("null_role", randomFrom((String[]) null, new String[0]),
                randomFrom((RoleDescriptor.IndicesPrivileges[]) null, new RoleDescriptor.IndicesPrivileges[0]),
                randomFrom((RoleDescriptor.ApplicationResourcePrivileges[])null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
                randomFrom((ConfigurableClusterPrivilege[])null, new ConfigurableClusterPrivilege[0]),
                randomFrom((String[])null, new String[0]),
                randomFrom((Map<String, Object>)null, Map.of()),
                Map.of("transient", "meta", "is", "ignored"));
        RoleDescriptor roleDescriptor1 = new RoleDescriptor("role_descriptor1", new String[]{"monitor"},
                new RoleDescriptor.IndicesPrivileges[]{RoleDescriptor.IndicesPrivileges.builder()
                        .indices("test*")
                        .privileges("read", "create_index")
                        .grantedFields("grantedField1")
                        .query("{\"match_all\":{}}")
                        .allowRestrictedIndices(true)
                        .build()},
                randomFrom((RoleDescriptor.ApplicationResourcePrivileges[]) null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
                randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
                randomFrom((String[]) null, new String[0]),
                randomFrom((Map<String, Object>) null, Map.of()),
                Map.of()
        );
        RoleDescriptor roleDescriptor2 = new RoleDescriptor("role_descriptor2", randomFrom((String[]) null, new String[0]),
                new RoleDescriptor.IndicesPrivileges[]{
                        RoleDescriptor.IndicesPrivileges.builder()
                                .indices("na\"me", "*")
                                .privileges("manage_ilm")
                                .deniedFields("denied*")
                                .query("{\"match\": {\"category\": \"click\"}}")
                                .build(),
                        RoleDescriptor.IndicesPrivileges.builder()
                                .indices("/@&~(\\.security.*)/")
                                .privileges("all", "cluster:a_wrong_*_one")
                                .build()},
                new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("maps")
                                .resources("raster:*")
                                .privileges("coming", "up", "with", "random", "names", "is", "hard")
                                .build()},
                randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
                new String[] {"impersonated???"},
                randomFrom((Map<String, Object>) null, Map.of()),
                Map.of()
        );
        RoleDescriptor roleDescriptor3 = new RoleDescriptor("role_descriptor3", randomFrom((String[]) null, new String[0]),
                randomFrom((RoleDescriptor.IndicesPrivileges[]) null, new RoleDescriptor.IndicesPrivileges[0]),
                new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("maps")
                                .resources("raster:*")
                                .privileges("{", "}", "\n", "\\", "\"")
                                .build(),
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                                .application("maps")
                                .resources("noooooo!!\n\n\f\\\\r", "{")
                                .privileges("*:*")
                                .build()},
                randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
                new String[] {"jack", "nich*", "//\""},
                Map.of("some meta", 42),
                Map.of()
        );
        Map<String, Object> metaMap = new TreeMap<>();
        metaMap.put("?list", List.of("e1", "e2", "*"));
        metaMap.put("some other meta", Map.of("r", "t"));
        RoleDescriptor roleDescriptor4 = new RoleDescriptor("role_descriptor4", new String[] {"manage_ml", "grant_api_key",
                "manage_rollup"},
                new RoleDescriptor.IndicesPrivileges[]{
                        RoleDescriptor.IndicesPrivileges.builder()
                                .indices("/. ? + * | { } [ ] ( ) \" \\/", "*")
                                .privileges("read", "read_cross_cluster")
                                .grantedFields("almost", "all*")
                                .deniedFields("denied*")
                                .build()},
                randomFrom((RoleDescriptor.ApplicationResourcePrivileges[]) null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
                new ConfigurableClusterPrivilege[] {
                        new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Set.of("a+b+|b+a+"))
                },
                new String[] {"//+a+\"[a]/"},
                metaMap,
                Map.of("ignored", 2)
        );
        String keyName = randomAlphaOfLength(4);
        TimeValue expiration = randomFrom(new TimeValue(randomNonNegativeLong(), randomFrom(TimeUnit.values())), null);
        List<RoleDescriptor> allTestRoleDescriptors = List.of(nullRoleDescriptor, roleDescriptor1, roleDescriptor2, roleDescriptor3,
                roleDescriptor4);
        List<RoleDescriptor> keyRoleDescriptors = randomSubsetOf(allTestRoleDescriptors);
        StringBuilder roleDescriptorsStringBuilder = new StringBuilder();
        roleDescriptorsStringBuilder.append("\"role_descriptors\":[");
        keyRoleDescriptors.forEach(roleDescriptor -> {
            roleDescriptorsStringBuilder.append(auditedRolesMap.get(roleDescriptor.getName()));
            roleDescriptorsStringBuilder.append(',');
        });
        if (false == keyRoleDescriptors.isEmpty()) {
            // delete last comma
            roleDescriptorsStringBuilder.deleteCharAt(roleDescriptorsStringBuilder.length() - 1);
        }
        roleDescriptorsStringBuilder.append("]");
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();

        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(keyName, keyRoleDescriptors, expiration);
        createApiKeyRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        auditTrail.accessGranted(requestId, authentication, CreateApiKeyAction.NAME, createApiKeyRequest, authorizationInfo);
        StringBuilder createKeyAuditEventStringBuilder = new StringBuilder();
        createKeyAuditEventStringBuilder.append("\"create\":{\"apikey\":{\"name\":\"" + keyName + "\",\"expiration\":" +
                (expiration != null ? "\"" + expiration.toString() + "\"" : "null") + ",");
        createKeyAuditEventStringBuilder.append(roleDescriptorsStringBuilder.toString());
        createKeyAuditEventStringBuilder.append("}}");
        String expectedCreateKeyAuditEventString = createKeyAuditEventStringBuilder.toString();
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedCreateKeyAuditEventString = output.get(1);
        assertThat(generatedCreateKeyAuditEventString, containsString(expectedCreateKeyAuditEventString));
        generatedCreateKeyAuditEventString = generatedCreateKeyAuditEventString.replace(", " + expectedCreateKeyAuditEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "create_apikey")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedCreateKeyAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        grantApiKeyRequest.getGrant().setType(randomFrom(randomAlphaOfLength(8), null));
        grantApiKeyRequest.getGrant().setUsername(randomFrom(randomAlphaOfLength(8), null));
        grantApiKeyRequest.getGrant().setPassword(randomFrom(new SecureString("password not exposed"), null));
        grantApiKeyRequest.getGrant().setAccessToken(randomFrom(new SecureString("access token not exposed"), null));
        grantApiKeyRequest.setApiKeyRequest(createApiKeyRequest);
        auditTrail.accessGranted(requestId, authentication, GrantApiKeyAction.NAME, grantApiKeyRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedGrantKeyAuditEventString = output.get(1);
        StringBuilder grantKeyAuditEventStringBuilder = new StringBuilder();
        grantKeyAuditEventStringBuilder.append("\"create\":{\"apikey\":{\"name\":\"" + keyName + "\",\"expiration\":" +
                (expiration != null ? "\"" + expiration.toString() + "\"" : "null") + ",");
        grantKeyAuditEventStringBuilder.append(roleDescriptorsStringBuilder.toString());
        grantKeyAuditEventStringBuilder.append("},\"grant\":{\"type\":");
        if (grantApiKeyRequest.getGrant().getType() != null) {
            grantKeyAuditEventStringBuilder.append("\"").append(grantApiKeyRequest.getGrant().getType()).append("\"");
        } else {
            grantKeyAuditEventStringBuilder.append("null");
        }
        if (grantApiKeyRequest.getGrant().getUsername() != null) {
            grantKeyAuditEventStringBuilder.append(",\"user\":{\"name\":\"").append(grantApiKeyRequest.getGrant().getUsername())
                    .append("\",\"has_password\":").append(grantApiKeyRequest.getGrant().getPassword() != null).append("}");
        }
        if (grantApiKeyRequest.getGrant().getAccessToken() != null) {
            grantKeyAuditEventStringBuilder.append(",\"has_access_token\":").append(true);
        }
        grantKeyAuditEventStringBuilder.append("}}");
        String expectedGrantKeyAuditEventString = grantKeyAuditEventStringBuilder.toString();
        assertThat(generatedGrantKeyAuditEventString, containsString(expectedGrantKeyAuditEventString));
        generatedGrantKeyAuditEventString = generatedGrantKeyAuditEventString.replace(", " + expectedGrantKeyAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "create_apikey")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedGrantKeyAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        PutRoleRequest putRoleRequest = new PutRoleRequest();
        putRoleRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        RoleDescriptor roleDescriptor = randomFrom(allTestRoleDescriptors);
        putRoleRequest.name(roleDescriptor.getName());
        putRoleRequest.cluster(roleDescriptor.getClusterPrivileges());
        putRoleRequest.addIndex(roleDescriptor.getIndicesPrivileges());
        putRoleRequest.runAs(roleDescriptor.getRunAs());
        putRoleRequest.conditionalCluster(roleDescriptor.getConditionalClusterPrivileges());
        putRoleRequest.addApplicationPrivileges(roleDescriptor.getApplicationPrivileges());
        putRoleRequest.metadata(roleDescriptor.getMetadata());
        auditTrail.accessGranted(requestId, authentication, PutRoleAction.NAME, putRoleRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedPutRoleAuditEventString = output.get(1);
        StringBuilder putRoleAuditEventStringBuilder = new StringBuilder();
        putRoleAuditEventStringBuilder.append("\"put\":{\"role\":{\"name\":\"" + putRoleRequest.name() + "\",")
                .append("\"role_descriptor\":")
                .append(auditedRolesMap.get(putRoleRequest.name()))
                .append("}}");
        String expectedPutRoleAuditEventString = putRoleAuditEventStringBuilder.toString();
        assertThat(generatedPutRoleAuditEventString, containsString(expectedPutRoleAuditEventString));
        generatedPutRoleAuditEventString = generatedPutRoleAuditEventString.replace(", " + expectedPutRoleAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_role")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutRoleAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest();
        deleteRoleRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        deleteRoleRequest.name(putRoleRequest.name());
        auditTrail.accessGranted(requestId, authentication, DeleteRoleAction.NAME, deleteRoleRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteRoleAuditEventString = output.get(1);
        StringBuilder deleteRoleStringBuilder = new StringBuilder();
        deleteRoleStringBuilder.append("\"delete\":{\"role\":{\"name\":");
        if (deleteRoleRequest.name() == null) {
            deleteRoleStringBuilder.append("null");
        } else {
            deleteRoleStringBuilder.append("\"").append(deleteRoleRequest.name()).append("\"");
        }
        deleteRoleStringBuilder.append("}}");
        String expectedDeleteRoleAuditEventString = deleteRoleStringBuilder.toString();
        assertThat(generatedDeleteRoleAuditEventString, containsString(expectedDeleteRoleAuditEventString));
        generatedDeleteRoleAuditEventString =
                generatedDeleteRoleAuditEventString.replace(", " + expectedDeleteRoleAuditEventString,"");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_role")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteRoleAuditEventString, checkedFields.immutableMap());
    }

    public void testSecurityConfigChangeEventFormattingForApiKeyInvalidation() throws IOException {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();

        final InvalidateApiKeyRequest invalidateApiKeyRequest = new InvalidateApiKeyRequest(
            randomFrom(randomAlphaOfLength(8), null),
            randomFrom(randomAlphaOfLength(8), null),
            randomFrom(randomAlphaOfLength(8), null),
            randomBoolean(),
            randomFrom(randomArray(1,3, String[]::new, () -> randomAlphaOfLength(8)), null)
        );

        auditTrail.accessGranted(requestId, authentication, InvalidateApiKeyAction.NAME, invalidateApiKeyRequest, authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedInvalidateKeyAuditEventString = output.get(1);
        StringBuilder invalidateKeyEventStringBuilder = new StringBuilder();
        invalidateKeyEventStringBuilder.append("\"invalidate\":{\"apikeys\":{");
        if (invalidateApiKeyRequest.getIds() != null && invalidateApiKeyRequest.getIds().length > 0) {
            invalidateKeyEventStringBuilder.append("\"ids\":[");
            for (String apiKeyId : invalidateApiKeyRequest.getIds()) {
                invalidateKeyEventStringBuilder.append("\"").append(apiKeyId).append("\",");
            }
            // delete last comma
            invalidateKeyEventStringBuilder.deleteCharAt(invalidateKeyEventStringBuilder.length() - 1);
            invalidateKeyEventStringBuilder.append("],");
        }
        if (Strings.hasLength(invalidateApiKeyRequest.getName())) {
            invalidateKeyEventStringBuilder.append("\"name\":\"").append(invalidateApiKeyRequest.getName()).append("\",");
        }
        invalidateKeyEventStringBuilder.append("\"owned_by_authenticated_user\":")
                .append(invalidateApiKeyRequest.ownedByAuthenticatedUser());
        if (Strings.hasLength(invalidateApiKeyRequest.getUserName()) || Strings.hasLength(invalidateApiKeyRequest.getRealmName())) {
            invalidateKeyEventStringBuilder.append(",\"user\":{\"name\":");
            if (Strings.hasLength(invalidateApiKeyRequest.getUserName())) {
                invalidateKeyEventStringBuilder.append("\"").append(invalidateApiKeyRequest.getUserName()).append("\"");
            } else {
                invalidateKeyEventStringBuilder.append("null");
            }
            invalidateKeyEventStringBuilder.append(",\"realm\":");
            if (Strings.hasLength(invalidateApiKeyRequest.getRealmName())) {
                invalidateKeyEventStringBuilder.append("\"").append(invalidateApiKeyRequest.getRealmName()).append("\"");
            } else {
                invalidateKeyEventStringBuilder.append("null");
            }
            invalidateKeyEventStringBuilder.append("}");
        }
        invalidateKeyEventStringBuilder.append("}}");
        String expectedInvalidateKeyEventString = invalidateKeyEventStringBuilder.toString();
        assertThat(generatedInvalidateKeyAuditEventString, containsString(expectedInvalidateKeyEventString));
        generatedInvalidateKeyAuditEventString = generatedInvalidateKeyAuditEventString
                .replace(", " + expectedInvalidateKeyEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "invalidate_apikeys")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedInvalidateKeyAuditEventString, checkedFields.immutableMap());
    }

    public void testSecurityConfigChangeEventFormattingForApplicationPrivileges() throws IOException {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        final Map<String, Object> metadata = new TreeMap<>();
        final String serializedMetadata;
        if (randomBoolean()) {
            metadata.put("test", true);
            metadata.put("ans", 42);
            serializedMetadata = ",\"metadata\":{\"ans\":42,\"test\":true}";
        } else {
            metadata.put("ans", List.of(42, true));
            metadata.put("other", Map.of("42", true));
            serializedMetadata = ",\"metadata\":{\"ans\":[42,true],\"other\":{\"42\":true}}";
        }

        PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest();
        putPrivilegesRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        putPrivilegesRequest.setPrivileges(Arrays.asList(randomArray(4, ApplicationPrivilegeDescriptor[]::new, () -> {
            Set<String> actions = Arrays.stream(generateRandomStringArray(4, 4, false)).collect(Collectors.toSet());
            return new ApplicationPrivilegeDescriptor(randomAlphaOfLength(4), randomAlphaOfLength(4), actions, metadata);
        })));
        auditTrail.accessGranted(requestId, authentication, PutPrivilegesAction.NAME, putPrivilegesRequest, authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedPutPrivilegesAuditEventString = output.get(1);
        StringBuilder putPrivilegesAuditEventStringBuilder = new StringBuilder();
        putPrivilegesAuditEventStringBuilder.append("\"put\":{\"privileges\":[");
        if (false == putPrivilegesRequest.getPrivileges().isEmpty()) {
            for (ApplicationPrivilegeDescriptor appPriv : putPrivilegesRequest.getPrivileges()) {
                putPrivilegesAuditEventStringBuilder.append("{\"application\":\"").append(appPriv.getApplication()).append("\"")
                        .append(",\"name\":\"").append(appPriv.getName()).append("\"")
                        .append(",\"actions\":[");
                if (appPriv.getActions().isEmpty()) {
                    putPrivilegesAuditEventStringBuilder.append("]");
                } else {
                    for (String action : appPriv.getActions()) {
                        putPrivilegesAuditEventStringBuilder.append("\"").append(action).append("\",");
                    }
                    // delete last comma
                    putPrivilegesAuditEventStringBuilder.deleteCharAt(putPrivilegesAuditEventStringBuilder.length() - 1);
                    putPrivilegesAuditEventStringBuilder.append("]");
                }
                putPrivilegesAuditEventStringBuilder.append(serializedMetadata).append("},");
            }
            // delete last comma
            putPrivilegesAuditEventStringBuilder.deleteCharAt(putPrivilegesAuditEventStringBuilder.length() - 1);
        }
        putPrivilegesAuditEventStringBuilder.append("]}");
        String expectedPutPrivilegesEventString = putPrivilegesAuditEventStringBuilder.toString();
        assertThat(generatedPutPrivilegesAuditEventString, containsString(expectedPutPrivilegesEventString));
        generatedPutPrivilegesAuditEventString = generatedPutPrivilegesAuditEventString
                .replace(", " + expectedPutPrivilegesEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_privileges")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutPrivilegesAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        DeletePrivilegesRequest deletePrivilegesRequest = new DeletePrivilegesRequest(randomFrom(randomAlphaOfLength(8), null),
                generateRandomStringArray(4, 4, true));
        deletePrivilegesRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        auditTrail.accessGranted(requestId, authentication, DeletePrivilegesAction.NAME, deletePrivilegesRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeletePrivilegesAuditEventString = output.get(1);
        StringBuilder deletePrivilegesAuditEventStringBuilder = new StringBuilder();
        deletePrivilegesAuditEventStringBuilder.append("\"delete\":{\"privileges\":{\"application\":");
        if (deletePrivilegesRequest.application() != null) {
            deletePrivilegesAuditEventStringBuilder.append("\"").append(deletePrivilegesRequest.application()).append("\"");
        } else {
            deletePrivilegesAuditEventStringBuilder.append("null");
        }
        deletePrivilegesAuditEventStringBuilder.append(",\"privileges\":");
        if (deletePrivilegesRequest.privileges() == null) {
            deletePrivilegesAuditEventStringBuilder.append("null");
        } else if (deletePrivilegesRequest.privileges().length == 0) {
            deletePrivilegesAuditEventStringBuilder.append("[]");
        } else {
            deletePrivilegesAuditEventStringBuilder.append("[");
            for (String privilege : deletePrivilegesRequest.privileges()) {
                if (privilege == null) {
                    deletePrivilegesAuditEventStringBuilder.append("null,");
                } else {
                    deletePrivilegesAuditEventStringBuilder.append("\"").append(privilege).append("\",");
                }
            }
            // delete last comma
            deletePrivilegesAuditEventStringBuilder.deleteCharAt(deletePrivilegesAuditEventStringBuilder.length() - 1);
            deletePrivilegesAuditEventStringBuilder.append("]");
        }
        deletePrivilegesAuditEventStringBuilder.append("}}");
        String expectedDeletePrivilegesEventString = deletePrivilegesAuditEventStringBuilder.toString();
        assertThat(generatedDeletePrivilegesAuditEventString, containsString(expectedDeletePrivilegesEventString));
        generatedDeletePrivilegesAuditEventString = generatedDeletePrivilegesAuditEventString
                .replace(", " + expectedDeletePrivilegesEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_privileges")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeletePrivilegesAuditEventString, checkedFields.immutableMap());
    }

    public void testSecurityConfigChangeEventFormattingForRoleMapping() throws IOException {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();

        PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest();
        putRoleMappingRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        putRoleMappingRequest.setName(randomFrom(randomAlphaOfLength(8), null));
        putRoleMappingRequest.setEnabled(randomBoolean());
        putRoleMappingRequest.setRoles(Arrays.asList(randomArray(4, String[]::new, () -> randomAlphaOfLength(4))));
        putRoleMappingRequest.setRoleTemplates(Arrays.asList(randomArray(4, TemplateRoleName[]::new,
                () -> new TemplateRoleName(new BytesArray(randomAlphaOfLengthBetween(0, 8)),
                        randomFrom(TemplateRoleName.Format.values())))));
        RoleMapperExpression mockRoleMapperExpression = new RoleMapperExpression() {
            @Override
            public boolean match(ExpressionModel model) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getWriteableName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("mock", "A mock role mapper expression");
                builder.endObject();
                return builder;
            }
        };
        boolean hasRules = randomBoolean();
        if (hasRules) {
            putRoleMappingRequest.setRules(mockRoleMapperExpression);
        }
        boolean hasMetadata = randomBoolean();
        if (hasMetadata) {
            Map<String, Object> metadata = new TreeMap<>();
            metadata.put("list", List.of("42", 13));
            metadata.put("smth", 42);
            putRoleMappingRequest.setMetadata(metadata);
        }
        auditTrail.accessGranted(requestId, authentication, PutRoleMappingAction.NAME, putRoleMappingRequest, authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedPutRoleMappingAuditEventString = output.get(1);
        StringBuilder putRoleMappingAuditEventStringBuilder = new StringBuilder();
        putRoleMappingAuditEventStringBuilder.append("\"put\":{\"role_mapping\":{\"name\":");
        if (putRoleMappingRequest.getName() != null) {
            putRoleMappingAuditEventStringBuilder.append("\"").append(putRoleMappingRequest.getName()).append("\"");
        } else {
            putRoleMappingAuditEventStringBuilder.append("null");
        }
        if (putRoleMappingRequest.getRoles() != null && false == putRoleMappingRequest.getRoles().isEmpty()) {
            putRoleMappingAuditEventStringBuilder.append(",\"roles\":[");
            for (String roleName : putRoleMappingRequest.getRoles()) {
                putRoleMappingAuditEventStringBuilder.append("\"").append(roleName).append("\",");
            }
            // delete last comma
            putRoleMappingAuditEventStringBuilder.deleteCharAt(putRoleMappingAuditEventStringBuilder.length() - 1);
            putRoleMappingAuditEventStringBuilder.append("]");
        }
        if (putRoleMappingRequest.getRoleTemplates() != null && false == putRoleMappingRequest.getRoleTemplates().isEmpty()) {
            putRoleMappingAuditEventStringBuilder.append(",\"role_templates\":[");
            for (TemplateRoleName templateRoleName : putRoleMappingRequest.getRoleTemplates()) {
                putRoleMappingAuditEventStringBuilder.append("{\"template\":\"")
                        .append(templateRoleName.getTemplate().utf8ToString())
                        .append("\",\"format\":\"")
                        .append(templateRoleName.getFormat().toString().toLowerCase(Locale.ROOT))
                        .append("\"},");
            }
            // delete last comma
            putRoleMappingAuditEventStringBuilder.deleteCharAt(putRoleMappingAuditEventStringBuilder.length() - 1);
            putRoleMappingAuditEventStringBuilder.append("]");
        }
        if (hasRules) {
            putRoleMappingAuditEventStringBuilder.append(",\"rules\":{\"mock\":\"A mock role mapper expression\"}");
        } else {
            putRoleMappingAuditEventStringBuilder.append(",\"rules\":null");
        }
        putRoleMappingAuditEventStringBuilder.append(",\"enabled\":").append(putRoleMappingRequest.isEnabled());
        if (hasMetadata) {
            putRoleMappingAuditEventStringBuilder.append(",\"metadata\":{\"list\":[\"42\",13],\"smth\":42}}}");
        } else {
            putRoleMappingAuditEventStringBuilder.append("}}");
        }
        String expectedPutRoleMappingAuditEventString = putRoleMappingAuditEventStringBuilder.toString();
        assertThat(generatedPutRoleMappingAuditEventString, containsString(expectedPutRoleMappingAuditEventString));
        generatedPutRoleMappingAuditEventString = generatedPutRoleMappingAuditEventString
                .replace(", " + expectedPutRoleMappingAuditEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_role_mapping")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutRoleMappingAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        DeleteRoleMappingRequest deleteRoleMappingRequest = new DeleteRoleMappingRequest();
        deleteRoleMappingRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        deleteRoleMappingRequest.setName(putRoleMappingRequest.getName());
        auditTrail.accessGranted(requestId, authentication, DeleteRoleMappingAction.NAME, deleteRoleMappingRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteRoleMappingAuditEventString = output.get(1);
        StringBuilder deleteRoleMappingStringBuilder = new StringBuilder();
        deleteRoleMappingStringBuilder.append("\"delete\":{\"role_mapping\":{\"name\":");
        if (deleteRoleMappingRequest.getName() == null) {
            deleteRoleMappingStringBuilder.append("null");
        } else {
            deleteRoleMappingStringBuilder.append("\"").append(deleteRoleMappingRequest.getName()).append("\"");
        }
        deleteRoleMappingStringBuilder.append("}}");
        String expectedDeleteRoleMappingAuditEventString = deleteRoleMappingStringBuilder.toString();
        assertThat(generatedDeleteRoleMappingAuditEventString, containsString(expectedDeleteRoleMappingAuditEventString));
        generatedDeleteRoleMappingAuditEventString =
                generatedDeleteRoleMappingAuditEventString.replace(", " + expectedDeleteRoleMappingAuditEventString,"");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_role_mapping")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteRoleMappingAuditEventString, checkedFields.immutableMap());
    }

    public void testSecurityConfigChangeEventFormattingForUsers() throws IOException {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();

        PutUserRequest putUserRequest = new PutUserRequest();
        String username = randomFrom(randomAlphaOfLength(3), customAnonymousUsername, AnonymousUser.DEFAULT_ANONYMOUS_USERNAME,
                UsernamesField.ELASTIC_NAME, UsernamesField.KIBANA_NAME);
        putUserRequest.username(username);
        putUserRequest.roles(randomFrom(randomArray(4, String[]::new, () -> randomAlphaOfLength(8)), null));
        putUserRequest.fullName(randomFrom(randomAlphaOfLength(8), null));
        putUserRequest.email(randomFrom(randomAlphaOfLength(8), null));
        putUserRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        putUserRequest.enabled(randomBoolean());
        putUserRequest.passwordHash(randomFrom(randomAlphaOfLengthBetween(0, 8).toCharArray(), null));
        boolean hasMetadata = randomBoolean();
        if (hasMetadata) {
            Map<String, Object> metadata = new TreeMap<>();
            metadata.put("smth", 42);
            metadata.put("list", List.of("42", 13));
            putUserRequest.metadata(metadata);
        }

        auditTrail.accessGranted(requestId, authentication, PutUserAction.NAME, putUserRequest, authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedPutUserAuditEventString = output.get(1);

        StringBuilder putUserAuditEventStringBuilder = new StringBuilder();
        putUserAuditEventStringBuilder.append("\"put\":{\"user\":{\"name\":");
        putUserAuditEventStringBuilder.append("\"" + putUserRequest.username() + "\"");
        putUserAuditEventStringBuilder.append(",\"enabled\":");
        putUserAuditEventStringBuilder.append(putUserRequest.enabled());
        putUserAuditEventStringBuilder.append(",\"roles\":");
        if (putUserRequest.roles() == null) {
            putUserAuditEventStringBuilder.append("null");
        } else if (putUserRequest.roles().length == 0) {
            putUserAuditEventStringBuilder.append("[]");
        } else {
            putUserAuditEventStringBuilder.append("[");
            for (String roleName : putUserRequest.roles()) {
                putUserAuditEventStringBuilder.append("\"").append(roleName).append("\",");
            }
            // delete last comma
            putUserAuditEventStringBuilder.deleteCharAt(putUserAuditEventStringBuilder.length() - 1);
            putUserAuditEventStringBuilder.append("]");
        }
        if (putUserRequest.fullName() != null) {
            putUserAuditEventStringBuilder.append(",\"full_name\":\"").append(putUserRequest.fullName()).append("\"");
        }
        if (putUserRequest.email() != null) {
            putUserAuditEventStringBuilder.append(",\"email\":\"").append(putUserRequest.email()).append("\"");
        }
        putUserAuditEventStringBuilder.append(",\"has_password\":").append(putUserRequest.passwordHash() != null);
        if (hasMetadata) {
            putUserAuditEventStringBuilder.append(",\"metadata\":{\"list\":[\"42\",13],\"smth\":42}}}");
        } else {
            putUserAuditEventStringBuilder.append("}}");
        }
        String expectedPutUserAuditEventString = putUserAuditEventStringBuilder.toString();
        assertThat(generatedPutUserAuditEventString, containsString(expectedPutUserAuditEventString));
        generatedPutUserAuditEventString = generatedPutUserAuditEventString.replace(", " + expectedPutUserAuditEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_user")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutUserAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        SetEnabledRequest setEnabledRequest = new SetEnabledRequest();
        setEnabledRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        // enable user
        setEnabledRequest.enabled(true);
        setEnabledRequest.username(username);
        auditTrail.accessGranted(requestId, authentication, SetEnabledAction.NAME, setEnabledRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedEnableUserAuditEventString = output.get(1);
        StringBuilder enableUserStringBuilder = new StringBuilder();
        enableUserStringBuilder.append("\"change\":{\"enable\":{\"user\":{\"name\":\"").append(username).append("\"}}}");
        String expectedEnableUserAuditEventString = enableUserStringBuilder.toString();
        assertThat(generatedEnableUserAuditEventString, containsString(expectedEnableUserAuditEventString));
        generatedEnableUserAuditEventString = generatedEnableUserAuditEventString.replace(", " + expectedEnableUserAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_enable_user")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedEnableUserAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        setEnabledRequest = new SetEnabledRequest();
        setEnabledRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        // disable user
        setEnabledRequest.enabled(false);
        setEnabledRequest.username(username);
        auditTrail.accessGranted(requestId, authentication, SetEnabledAction.NAME, setEnabledRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDisableUserAuditEventString = output.get(1);
        StringBuilder disableUserStringBuilder = new StringBuilder();
        disableUserStringBuilder.append("\"change\":{\"disable\":{\"user\":{\"name\":\"").append(username).append("\"}}}");
        String expectedDisableUserAuditEventString = disableUserStringBuilder.toString();
        assertThat(generatedDisableUserAuditEventString, containsString(expectedDisableUserAuditEventString));
        generatedDisableUserAuditEventString = generatedDisableUserAuditEventString.replace(", " + expectedDisableUserAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_disable_user")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDisableUserAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        ChangePasswordRequest changePasswordRequest = new ChangePasswordRequest();
        changePasswordRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        changePasswordRequest.username(username);
        changePasswordRequest.passwordHash(randomFrom(randomAlphaOfLengthBetween(0, 8).toCharArray(), null));
        auditTrail.accessGranted(requestId, authentication, ChangePasswordAction.NAME, changePasswordRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedChangePasswordAuditEventString = output.get(1);
        StringBuilder changePasswordStringBuilder = new StringBuilder();
        changePasswordStringBuilder.append("\"change\":{\"password\":{\"user\":{\"name\":\"").append(username).append("\"}}}");
        String expectedChangePasswordAuditEventString = changePasswordStringBuilder.toString();
        assertThat(generatedChangePasswordAuditEventString, containsString(expectedChangePasswordAuditEventString));
        generatedChangePasswordAuditEventString =
                generatedChangePasswordAuditEventString.replace(", " + expectedChangePasswordAuditEventString,
                "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_password")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedChangePasswordAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        DeleteUserRequest deleteUserRequest = new DeleteUserRequest();
        deleteUserRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        deleteUserRequest.username(username);
        auditTrail.accessGranted(requestId, authentication, DeleteUserAction.NAME, deleteUserRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteUserAuditEventString = output.get(1);
        StringBuilder deleteUserStringBuilder = new StringBuilder();
        deleteUserStringBuilder.append("\"delete\":{\"user\":{\"name\":\"").append(username).append("\"}}");
        String expectedDeleteUserAuditEventString = deleteUserStringBuilder.toString();
        assertThat(generatedDeleteUserAuditEventString, containsString(expectedDeleteUserAuditEventString));
        generatedDeleteUserAuditEventString =
                generatedDeleteUserAuditEventString.replace(", " + expectedDeleteUserAuditEventString,"");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
                .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_user")
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteUserAuditEventString, checkedFields.immutableMap());
    }

    public void testSecurityConfigChangeEventFormattingForServiceAccountToken() {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();

        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
            namespace, serviceName, tokenName);

        auditTrail.accessGranted(requestId, authentication, CreateServiceAccountTokenAction.NAME,
            createServiceAccountTokenRequest, authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedCreateServiceAccountTokenAuditEventString = output.get(1);

        final String expectedCreateServiceAccountTokenAuditEventString =
            String.format(Locale.ROOT,
                "\"create\":{\"service_token\":{\"namespace\":\"%s\",\"service\":\"%s\",\"name\":\"%s\"}}",
                namespace, serviceName, tokenName);
        assertThat(generatedCreateServiceAccountTokenAuditEventString, containsString(expectedCreateServiceAccountTokenAuditEventString));
        generatedCreateServiceAccountTokenAuditEventString =
            generatedCreateServiceAccountTokenAuditEventString.replace(", " + expectedCreateServiceAccountTokenAuditEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "create_service_token")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedCreateServiceAccountTokenAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest =
            new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName);

        auditTrail.accessGranted(requestId, authentication, DeleteServiceAccountTokenAction.NAME,
            deleteServiceAccountTokenRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteServiceAccountTokenAuditEventString = output.get(1);

        final String expectedDeleteServiceAccountTokenAuditEventString =
            String.format(Locale.ROOT,
                "\"delete\":{\"service_token\":{\"namespace\":\"%s\",\"service\":\"%s\",\"name\":\"%s\"}}",
                namespace, serviceName, tokenName);
        assertThat(generatedDeleteServiceAccountTokenAuditEventString, containsString(expectedDeleteServiceAccountTokenAuditEventString));
        generatedDeleteServiceAccountTokenAuditEventString =
            generatedDeleteServiceAccountTokenAuditEventString.replace(", " + expectedDeleteServiceAccountTokenAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_service_token")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteServiceAccountTokenAuditEventString, checkedFields.immutableMap());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
    }

    public void testAnonymousAccessDeniedTransport() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.anonymousAccessDenied(requestId, "_action", request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        indicesRequest(request, checkedFields, checkedArrayFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied")
                .build());
        auditTrail.anonymousAccessDenied(requestId, "_action", request);
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
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied")
                .build());
        auditTrail.anonymousAccessDenied(requestId, request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailed() throws Exception {
        final AuthenticationToken authToken = createAuthenticationToken();
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, authToken, "_action", request);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authToken.principal())
                     .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        if (authToken instanceof ServiceAccountToken) {
            checkedFields.put(LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME, ((ServiceAccountToken) authToken).getTokenName());
        }
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build());
        auditTrail.authenticationFailed(requestId, createAuthenticationToken(), "_action", request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, "_action", request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                     .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build());
        auditTrail.authenticationFailed(requestId, "_action", request);
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
        final AuthenticationToken authToken = createAuthenticationToken();

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, authToken, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authToken.principal())
                     .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                     .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
                     .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (authToken instanceof ServiceAccountToken) {
            checkedFields.put(LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME, ((ServiceAccountToken) authToken).getTokenName());
        }
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
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build());
        auditTrail.authenticationFailed(requestId, createAuthenticationToken(), request);
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
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "authentication_failed")
                .build());
        auditTrail.authenticationFailed(requestId, request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRealm() throws Exception {
        final AuthenticationToken authToken = mockToken();
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String realm = randomAlphaOfLengthBetween(1, 6);
        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, realm, authToken, "_action", request);
        assertEmptyLog(logger);

        // test enabled
        updateLoggerSettings(Settings.builder()
                       .put(settings)
                       .put("xpack.security.audit.logfile.events.include", "realm_authentication_failed")
                       .build());
        auditTrail.authenticationFailed(requestId, realm, authToken, "_action", request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                     .put(LoggingAuditTrail.REALM_FIELD_NAME, realm)
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authToken.principal())
                     .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                     .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                     .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
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
        final AuthenticationToken authToken = mockToken();
        final String realm = randomAlphaOfLengthBetween(1, 6);
        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, realm, authToken, request);
        assertEmptyLog(logger);

        // test enabled
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "realm_authentication_failed")
                .build());
        auditTrail.authenticationFailed(requestId, realm, authToken, request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "realm_authentication_failed")
                     .put(LoggingAuditTrail.REALM_FIELD_NAME, realm)
                     .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
                     .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authToken.principal())
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
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final String requestId = randomRequestId();
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();

        Authentication authentication = createAuthentication();
        auditTrail.accessGranted(requestId, authentication, "_action", request, authorizationInfo);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);

        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        checkedFields = new MapBuilder<>(commonFields);
        checkedArrayFields = new MapBuilder<>();
        auditTrail.accessGranted(requestId, authentication, "_action", request, authorizationInfo);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "access_granted")
                .build());
        auditTrail.accessGranted(requestId, authentication, "_action", request, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testSecurityConfigChangedEventSelection() {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        Tuple<String, TransportRequest> actionAndRequest = randomFrom(new Tuple<>(PutUserAction.NAME, new PutUserRequest()),
                new Tuple<>(PutRoleAction.NAME, new PutRoleRequest()),
                new Tuple<>(PutRoleMappingAction.NAME, new PutRoleMappingRequest()),
                new Tuple<>(SetEnabledAction.NAME, new SetEnabledRequest()),
                new Tuple<>(ChangePasswordAction.NAME, new ChangePasswordRequest()),
                new Tuple<>(CreateApiKeyAction.NAME, new CreateApiKeyRequest()),
                new Tuple<>(GrantApiKeyAction.NAME, new GrantApiKeyRequest()),
                new Tuple<>(PutPrivilegesAction.NAME, new PutPrivilegesRequest()),
                new Tuple<>(DeleteUserAction.NAME, new DeleteUserRequest()),
                new Tuple<>(DeleteRoleAction.NAME, new DeleteRoleRequest()),
                new Tuple<>(DeleteRoleMappingAction.NAME, new DeleteRoleMappingRequest()),
                new Tuple<>(InvalidateApiKeyAction.NAME, new InvalidateApiKeyRequest()),
                new Tuple<>(DeletePrivilegesAction.NAME, new DeletePrivilegesRequest()),
                new Tuple<>(CreateServiceAccountTokenAction.NAME, new CreateServiceAccountTokenRequest(namespace, serviceName, tokenName)),
                new Tuple<>(DeleteServiceAccountTokenAction.NAME, new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName))
        );
        auditTrail.accessGranted(requestId, authentication, actionAndRequest.v1(), actionAndRequest.v2(), authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        assertThat(output.get(1), containsString("security_config_change"));
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "security_config_change")
                .build());
        auditTrail.accessGranted(requestId, authentication, actionAndRequest.v1(), actionAndRequest.v2(), authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), not(containsString("security_config_change")));
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "security_config_change")
                .put("xpack.security.audit.logfile.events.exclude", "access_granted")
                .build());
        auditTrail.accessGranted(requestId, authentication, actionAndRequest.v1(), actionAndRequest.v2(), authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), containsString("security_config_change"));
    }

    public void testSystemAccessGranted() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final User systemUser = randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE);
        final Authentication authentication = new Authentication(systemUser, new RealmRef("_reserved", "test", "foo"), null);
        final String requestId = randomRequestId();

        auditTrail.accessGranted(requestId, authentication, "_action", request, authorizationInfo);
        // system user
        assertEmptyLog(logger);
        auditTrail.explicitIndexAccessEvent(requestId, randomFrom(AuditLevel.ACCESS_GRANTED, AuditLevel.SYSTEM_ACCESS_GRANTED),
                authentication, "_action", randomFrom(randomAlphaOfLengthBetween(1, 4), null),
                BulkItemRequest.class.getName(),
                request.remoteAddress(),
                authorizationInfo);
        // system user
        assertEmptyLog(logger);

        // enable system user for access granted events
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "system_access_granted")
                .build());

        auditTrail.accessGranted(requestId, authentication, "_action", request, authorizationInfo);

        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());
        clearLog();

        String index = randomFrom(randomAlphaOfLengthBetween(1, 4), null);
        auditTrail.explicitIndexAccessEvent(requestId, randomFrom(AuditLevel.ACCESS_GRANTED, AuditLevel.SYSTEM_ACCESS_GRANTED),
                authentication, "_action", index, BulkItemRequest.class.getName(), request.remoteAddress(), authorizationInfo);

        checkedFields = new MapBuilder<>(commonFields);
        checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, BulkItemRequest.class.getName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        if (index != null) {
            checkedArrayFields.put(LoggingAuditTrail.INDICES_FIELD_NAME, new String[]{index});
        }
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());
    }

    public void testAccessGrantedInternalSystemAction() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final User systemUser = randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE);
        final Authentication authentication = new Authentication(systemUser, new RealmRef("_reserved", "test", "foo"), null);
        final String requestId = randomRequestId();
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        assertEmptyLog(logger);

        // test enabled
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "system_access_granted")
                .build());
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME, AuthenticationType.REALM.toString())
                .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, systemUser.principal())
                .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, "_reserved")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "internal:_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());
    }

    public void testAccessGrantedInternalSystemActionNonSystemUser() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final String requestId = randomRequestId();
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();

        Authentication authentication = createAuthentication();
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "internal:_action")
            .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        checkedFields = new MapBuilder<>(commonFields);
        checkedArrayFields = new MapBuilder<>();
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "internal:_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "access_granted")
                .build());
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testAccessDenied() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final String requestId = randomRequestId();
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();

        Authentication authentication = createAuthentication();
        auditTrail.accessDenied(requestId, authentication, "_action/bar", request, authorizationInfo);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_denied")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action/bar")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        if (authentication.isServiceAccount()) {
            checkedFields.put(LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME, (String) authentication.getMetadata().get(TOKEN_NAME_FIELD))
                .put(LoggingAuditTrail.SERVICE_TOKEN_TYPE_FIELD_NAME,
                    ServiceAccountSettings.REALM_TYPE + "_" + authentication.getMetadata().get(TOKEN_SOURCE_FIELD));
        }
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        checkedFields = new MapBuilder<>(commonFields);
        checkedArrayFields = new MapBuilder<>();
        auditTrail.accessDenied(requestId, authentication, "_action/bar", request, authorizationInfo);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_denied")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action/bar")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "access_denied")
                .build());
        auditTrail.accessDenied(requestId, authentication, "_action", request, authorizationInfo);
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
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "tampered_request")
                .build());
        auditTrail.tamperedRequest(requestId, request);
        assertEmptyLog(logger);
    }

    public void testTamperedRequest() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);

        final String requestId = randomRequestId();
        auditTrail.tamperedRequest(requestId, "_action", request);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "tampered_request")
                .build());
        auditTrail.tamperedRequest(requestId, "_action", request);
        assertEmptyLog(logger);
    }

    public void testTamperedRequestWithUser() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String requestId = randomRequestId();
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();

        Authentication authentication = createAuthentication();
        auditTrail.tamperedRequest(requestId, authentication, "_action", request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        checkedFields = new MapBuilder<>(commonFields);
        checkedArrayFields = new MapBuilder<>();
        auditTrail.tamperedRequest(requestId, authentication, "_action", request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "tampered_request")
                .build());
        auditTrail.tamperedRequest(requestId, authentication, "_action", request);
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
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "connection_denied")
                .build());
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
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "connection_granted")
                .build());
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
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = new Authentication(
                new User("running as", new String[] { "r2" }, new User("_username", new String[] { "r1" })),
                new RealmRef("authRealm", "test", "foo"),
                new RealmRef("lookRealm", "up", "by"));
        final String requestId = randomRequestId();

        auditTrail.runAsGranted(requestId, authentication, "_action", request, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "run_as_granted")
                .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username")
                .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, "authRealm")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME, "running as")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME, "lookRealm")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "run_as_granted")
                .build());
        auditTrail.runAsGranted(requestId, authentication, "_action", request, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testRunAsDenied() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = new Authentication(
                new User("running as", new String[] { "r2" }, new User("_username", new String[] { "r1" })),
                new RealmRef("authRealm", "test", "foo"),
                new RealmRef("lookRealm", "up", "by"));
        final String requestId = randomRequestId();

        auditTrail.runAsDenied(requestId, authentication, "_action", request, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "run_as_denied")
                .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username")
                .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, "authRealm")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME, "running as")
                .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME, "lookRealm")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.exclude", "run_as_denied")
                .build());
        auditTrail.runAsDenied(requestId, authentication, "_action", request, authorizationInfo);
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
        final String requestId = randomRequestId();
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        Authentication authentication = createAuthentication();

        // event by default disabled
        auditTrail.authenticationSuccess(requestId, authentication, request);
        assertEmptyLog(logger);

        updateLoggerSettings(Settings.builder()
                .put(this.settings)
                .put("xpack.security.audit.logfile.events.include", "authentication_success")
                .build());
        auditTrail.authenticationSuccess(requestId, authentication, request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                     .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
                     .put(LoggingAuditTrail.REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName())
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
        authentication(authentication, checkedFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        checkedFields = new MapBuilder<>(commonFields);
        auditTrail.authenticationSuccess(requestId, authentication, request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
                .put(LoggingAuditTrail.REALM_FIELD_NAME, "_es_api_key")
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
        authentication(authentication, checkedFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap());
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String requestId = randomRequestId();
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        Authentication authentication = createAuthentication();

        // event by default disabled
        auditTrail.authenticationSuccess(requestId, authentication, "_action", request);
        assertEmptyLog(logger);

        updateLoggerSettings(Settings.builder()
                .put(this.settings)
                .put("xpack.security.audit.logfile.events.include", "authentication_success")
                .build());
        auditTrail.authenticationSuccess(requestId, authentication, "_action", request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.immutableMap(), checkedArrayFields.immutableMap());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        checkedFields = new MapBuilder<>(commonFields);
        checkedArrayFields = new MapBuilder<>();
        auditTrail.authenticationSuccess(requestId, authentication, "_action", request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
                .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
                .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
                .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
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
        final AuthorizationInfo authorizationInfo =
            () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, new String[] { randomAlphaOfLengthBetween(1, 6) });
        final String realm = randomAlphaOfLengthBetween(1, 6);
        // transport messages without indices
        final TransportRequest[] requests = new TransportRequest[] { new MockRequest(threadContext),
                new org.elasticsearch.action.MockIndicesRequest(IndicesOptions.strictExpandOpenAndForbidClosed(), new String[0]),
                new org.elasticsearch.action.MockIndicesRequest(IndicesOptions.strictExpandOpenAndForbidClosed(), (String[]) null) };
        final List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        int logEntriesCount = 1;
        for (final TransportRequest request : requests) {
            auditTrail.anonymousAccessDenied("_req_id", "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationFailed("_req_id", createAuthenticationToken(), "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationFailed("_req_id", "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationFailed("_req_id", realm, mockToken(), "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.accessGranted("_req_id", randomBoolean() ? createAuthentication() : createApiKeyAuthentication(apiKeyService,
                    createAuthentication()), "_action", request, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.accessDenied("_req_id", randomBoolean() ? createAuthentication() : createApiKeyAuthentication(apiKeyService,
                    createAuthentication()), "_action", request, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.tamperedRequest("_req_id", "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.tamperedRequest("_req_id", randomBoolean() ? createAuthentication() : createApiKeyAuthentication(apiKeyService,
                    createAuthentication()), "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.runAsGranted("_req_id", randomBoolean() ? createAuthentication() : createApiKeyAuthentication(apiKeyService,
                    createAuthentication()), "_action", request, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.runAsDenied("_req_id", randomBoolean() ? createAuthentication() : createApiKeyAuthentication(apiKeyService,
                    createAuthentication()), "_action", request, authorizationInfo);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationSuccess("_req_id", randomBoolean() ? createAuthentication() :
                            createApiKeyAuthentication(apiKeyService, createAuthentication()),
                    "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
        }
    }

    private void updateLoggerSettings(Settings settings) {
        this.settings = settings;
        // either create a new audit trail or update the settings on the existing one
        if (randomBoolean()) {
            this.auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        } else {
            this.clusterService.getClusterSettings().applySettings(settings);
        }
    }

    private void assertMsg(Logger logger, Map<String, String> checkFields) {
        assertMsg(logger, checkFields, Collections.emptyMap());
    }

    private void assertMsg(String logLine, Map<String, String> checkFields) {
        assertMsg(logLine, checkFields, Collections.emptyMap());
    }

    private void assertMsg(Logger logger, Map<String, String> checkFields, Map<String, String[]> checkArrayFields) {
        final List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat("Exactly one logEntry expected. Found: " + output.size(), output.size(), is(1));
        if (checkFields == null) {
            // only check msg existence
            return;
        }
        String logLine = output.get(0);
        assertMsg(logLine, checkFields, checkArrayFields);
    }

    private void assertMsg(String logLine, Map<String, String> checkFields, Map<String, String[]> checkArrayFields) {
        // check each string-valued field
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
        // check each array-valued field
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

    private Authentication createAuthentication() {
        final RealmRef lookedUpBy;
        final RealmRef authBy;
        final User user;
        final AuthenticationType authenticationType;
        final Map<String, Object> authMetadata;
        switch (randomIntBetween(0, 2)) {
            case 0:
                user = new User(randomAlphaOfLength(4), new String[] { "r1" }, new User("authenticated_username", "r2"));
                lookedUpBy = new RealmRef(randomAlphaOfLength(4), "lookup", "by");
                authBy = new RealmRef("authRealm", "auth", "foo");
                authenticationType= randomFrom(AuthenticationType.REALM, AuthenticationType.TOKEN,
                    AuthenticationType.INTERNAL, AuthenticationType.ANONYMOUS);
                authMetadata = Map.of();
                break;
            case 1:
                user = new User(randomAlphaOfLength(4), "r1");
                lookedUpBy = null;
                authBy = new RealmRef(randomAlphaOfLength(4), "auth", "by");
                authenticationType= randomFrom(AuthenticationType.REALM, AuthenticationType.TOKEN,
                    AuthenticationType.INTERNAL, AuthenticationType.ANONYMOUS);
                authMetadata = Map.of();
                break;
            default:  // service account
                final String principal = randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
                user = new User(principal, Strings.EMPTY_ARRAY, "Service account - " + principal, null,
                    Map.of("_elastic_service_account", true), true);
                lookedUpBy = null;
                authBy = new RealmRef("_service_account", "_service_account", randomAlphaOfLengthBetween(3, 8));
                authenticationType = AuthenticationType.TOKEN;
                final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
                authMetadata = Map.of("_token_name", ValidationTests.randomTokenName(),
                    "_token_source", tokenSource.name().toLowerCase(Locale.ROOT));
        }
        return new Authentication(user, authBy, lookedUpBy, Version.CURRENT, authenticationType, authMetadata);
    }

    private AuthenticationToken createAuthenticationToken() {
        switch (randomIntBetween(0, 2)) {
            case 0:
                final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
                return ServiceAccountToken.newToken(accountId, ValidationTests.randomTokenName());
            default:
                return mockToken();
        }
    }

    private AuthenticationToken mockToken() {
        return new AuthenticationToken() {
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
            public void clearCredentials() { }
        };
    }

    private ClusterSettings mockClusterSettings() {
        final List<Setting<?>> settingsList = new ArrayList<>();
        LoggingAuditTrail.registerSettings(settingsList);
        settingsList.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ClusterSettings(settings, new HashSet<>(settingsList));
    }

    static class MockRequest extends TransportRequest {

        MockRequest(ThreadContext threadContext) throws IOException {
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

    /**
     * @return A tuple of ( id-to-pass-to-audit-trail, id-to-check-in-audit-message )
     */
    private String randomRequestId() {
        return randomBoolean() ? randomAlphaOfLengthBetween(8, 24) : AuditUtil.generateRequestId(threadContext);
    }

    private static void restOrTransportOrigin(TransportRequest request, ThreadContext threadContext,
                                              MapBuilder<String, String> checkedFields) {
        final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress != null) {
            checkedFields.put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                    .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
        } else {
            final TransportAddress address = request.remoteAddress();
            if (address != null) {
                checkedFields.put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                        .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address.address()));
            }
        }
    }

    private static void authentication(Authentication authentication, MapBuilder<String, String> checkedFields) {
        checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authentication.getUser().principal());
        checkedFields.put(LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME, authentication.getAuthenticationType().toString());
        if (Authentication.AuthenticationType.API_KEY == authentication.getAuthenticationType()) {
            assert false == authentication.getUser().isRunAs();
            checkedFields.put(LoggingAuditTrail.API_KEY_ID_FIELD_NAME,
                    (String) authentication.getMetadata().get(ApiKeyService.API_KEY_ID_KEY));
            String apiKeyName = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_NAME_KEY);
            if (apiKeyName != null) {
                checkedFields.put(LoggingAuditTrail.API_KEY_NAME_FIELD_NAME, apiKeyName);
            }
            String creatorRealmName = (String) authentication.getMetadata().get(ApiKeyService.API_KEY_CREATOR_REALM_NAME);
            if (creatorRealmName != null) {
                checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, creatorRealmName);
            }

        } else {
            if (authentication.getUser().isRunAs()) {
                checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authentication.getLookedUpBy().getName())
                        .put(LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME, authentication.getUser().authenticatedUser().principal())
                        .put(LoggingAuditTrail.PRINCIPAL_RUN_BY_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
            } else {
                checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authentication.getAuthenticatedBy().getName());
            }
        }
        if (authentication.isServiceAccount()) {
            checkedFields.put(LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME, (String) authentication.getMetadata().get(TOKEN_NAME_FIELD))
                .put(LoggingAuditTrail.SERVICE_TOKEN_TYPE_FIELD_NAME,
                    ServiceAccountSettings.REALM_TYPE + "_" + authentication.getMetadata().get(TOKEN_SOURCE_FIELD));
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

    private static void indicesRequest(TransportRequest request, MapBuilder<String, String> checkedFields,
                                       MapBuilder<String, String[]> checkedArrayFields) {
        if (request instanceof IndicesRequest) {
            checkedFields.put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, MockIndicesRequest.class.getSimpleName());
            checkedArrayFields.put(LoggingAuditTrail.INDICES_FIELD_NAME, ((IndicesRequest) request).indices());
        } else {
            checkedFields.put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, MockRequest.class.getSimpleName());
        }
    }

}
