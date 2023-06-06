/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import io.netty.channel.Channel;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.FakeRestRequest.Builder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledAction;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledRequest;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
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
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
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
import org.mockito.Mockito;
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
import java.util.LinkedHashSet;
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
import static org.hamcrest.Matchers.oneOf;
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

    // Test settings assertion flow: init() => new LoggingAuditTrail.EntryCommonFields() => commonFields => test*() => assertMsg()
    @Before
    public void init() throws Exception {
        includeRequestBody = randomBoolean();
        settings = Settings.builder()
            .put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), randomBoolean())
            .put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), randomBoolean())
            .put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), randomBoolean())
            .put(LoggingAuditTrail.EMIT_NODE_ID_SETTING.getKey(), randomBoolean())
            .put(LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING.getKey(), randomBoolean())
            .put(LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING.getKey(), randomBoolean())
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), randomAlphaOfLength(16))
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
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
            .metadata(Metadata.builder().clusterUUID(UUIDs.randomBase64UUID()).build())
            .build();
        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getClusterName()).thenReturn(ClusterName.CLUSTER_NAME_SETTING.get(settings));
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        when(clusterService.state()).thenReturn(clusterState);
        Mockito.doAnswer((Answer) invocation -> {
            final LoggingAuditTrail arg0 = (LoggingAuditTrail) invocation.getArguments()[0];
            arg0.updateLocalNodeInfo(localNode);
            return null;
        }).when(clusterService).addListener(Mockito.isA(LoggingAuditTrail.class));
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING,
                LoggingAuditTrail.EMIT_HOST_NAME_SETTING,
                LoggingAuditTrail.EMIT_NODE_NAME_SETTING,
                LoggingAuditTrail.EMIT_NODE_ID_SETTING,
                LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING,
                LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING,
                LoggingAuditTrail.INCLUDE_EVENT_SETTINGS,
                LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS,
                LoggingAuditTrail.INCLUDE_REQUEST_BODY,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_INDICES,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_ACTIONS,
                Loggers.LOG_LEVEL_SETTING,
                ApiKeyService.DELETE_RETENTION_PERIOD
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        // This constructor call is a filter. It uses emit settings to use (or ignore) common fields for all audit logs.
        final LoggingAuditTrail.EntryCommonFields entryCommonFields = new LoggingAuditTrail.EntryCommonFields(
            settings,
            localNode,
            clusterService
        );
        // This filtered map contains audit log assertions to be used by all tests during calls to assertMsg(). Null values assert absence.
        commonFields = entryCommonFields.commonFields;
        threadContext = new ThreadContext(Settings.EMPTY);
        if (randomBoolean()) {
            threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, randomAlphaOfLengthBetween(1, 4));
        }
        if (randomBoolean()) {
            threadContext.putHeader(Task.TRACE_ID, randomAlphaOfLength(32));
        }
        if (randomBoolean()) {
            threadContext.putHeader(
                AuditTrail.X_FORWARDED_FOR_HEADER,
                randomFrom("2001:db8:85a3:8d3:1319:8a2e:370:7348", "203.0.113.195", "203.0.113.195, 70.41.3.18, 150.172.238.178")
            );
        }
        logger = CapturingLogger.newCapturingLogger(randomFrom(Level.OFF, Level.FATAL, Level.ERROR, Level.WARN, Level.INFO), patternLayout);
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        apiKeyService = new ApiKeyService(
            settings,
            Clock.systemUTC(),
            client,
            securityIndexManager,
            clusterService,
            mock(CacheInvalidatorRegistry.class),
            mock(ThreadPool.class)
        );
    }

    @After
    public void clearLog() throws Exception {
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
    }

    public void testEventsSettingValidation() {
        final String prefix = "xpack.security.audit.logfile.events.";
        Settings settings = Settings.builder().putList(prefix + "include", Arrays.asList("access_granted", "bogus")).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.get(settings)
        );
        assertThat(e, hasToString(containsString("invalid event name specified [bogus]")));

        Settings settings2 = Settings.builder().putList(prefix + "exclude", Arrays.asList("access_denied", "foo")).build();
        e = expectThrows(IllegalArgumentException.class, () -> LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS.get(settings2));
        assertThat(e, hasToString(containsString("invalid event name specified [foo]")));
    }

    public void testAuditFilterSettingValidation() {
        final String prefix = "xpack.security.audit.logfile.events.";
        Settings settings = Settings.builder().putList(prefix + "ignore_filters.filter1.users", Arrays.asList("mickey", "/bogus")).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace("filter1").get(settings)
        );
        assertThat(e, hasToString(containsString("invalid pattern [/bogus]")));

        Settings settings2 = Settings.builder()
            .putList(prefix + "ignore_filters.filter2.users", Arrays.asList("tom", "cruise"))
            .putList(prefix + "ignore_filters.filter2.realms", Arrays.asList("native", "/foo"))
            .build();
        assertThat(
            LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace("filter2").get(settings2),
            containsInAnyOrder("tom", "cruise")
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace("filter2").get(settings2)
        );
        assertThat(e, hasToString(containsString("invalid pattern [/foo]")));

        Settings settings3 = Settings.builder()
            .putList(prefix + "ignore_filters.filter3.realms", Arrays.asList("native", "oidc1"))
            .putList(prefix + "ignore_filters.filter3.roles", Arrays.asList("kibana", "/wrong"))
            .build();
        assertThat(
            LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS.getConcreteSettingForNamespace("filter3").get(settings3),
            containsInAnyOrder("native", "oidc1")
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace("filter3").get(settings3)
        );
        assertThat(e, hasToString(containsString("invalid pattern [/wrong]")));

        Settings settings4 = Settings.builder()
            .putList(prefix + "ignore_filters.filter4.roles", Arrays.asList("kibana", "elastic"))
            .putList(prefix + "ignore_filters.filter4.indices", Arrays.asList("index-1", "/no-inspiration"))
            .build();
        assertThat(
            LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES.getConcreteSettingForNamespace("filter4").get(settings4),
            containsInAnyOrder("kibana", "elastic")
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_INDICES.getConcreteSettingForNamespace("filter4").get(settings4)
        );
        assertThat(e, hasToString(containsString("invalid pattern [/no-inspiration]")));

        Settings settings5 = Settings.builder()
            .putList(prefix + "ignore_filters.filter2.users", Arrays.asList("tom", "cruise"))
            .putList(prefix + "ignore_filters.filter2.actions", Arrays.asList("indices:data/read/*", "/foo"))
            .build();
        assertThat(
            LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS.getConcreteSettingForNamespace("filter2").get(settings5),
            containsInAnyOrder("tom", "cruise")
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> LoggingAuditTrail.FILTER_POLICY_IGNORE_ACTIONS.getConcreteSettingForNamespace("filter2").get(settings5)
        );
        assertThat(e, hasToString(containsString("invalid pattern [/foo]")));
    }

    public void testSecurityConfigChangeEventFormattingForRoles() throws IOException {
        final Path path = getDataPath("/org/elasticsearch/xpack/security/audit/logfile/audited_roles.txt");
        final Map<String, String> auditedRolesMap = new HashMap<>();
        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(new BufferedInputStream(Files.newInputStream(path)), StandardCharsets.UTF_8)
            )
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                // even number of lines
                auditedRolesMap.put(line, reader.readLine());
            }
        }

        RoleDescriptor nullRoleDescriptor = new RoleDescriptor(
            "null_role",
            randomFrom((String[]) null, new String[0]),
            randomFrom((RoleDescriptor.IndicesPrivileges[]) null, new RoleDescriptor.IndicesPrivileges[0]),
            randomFrom((RoleDescriptor.ApplicationResourcePrivileges[]) null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
            randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
            randomFrom((String[]) null, new String[0]),
            randomFrom((Map<String, Object>) null, Map.of()),
            Map.of("transient", "meta", "is", "ignored")
        );
        RoleDescriptor roleDescriptor1 = new RoleDescriptor(
            "role_descriptor1",
            new String[] { "monitor" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("test*")
                    .privileges("read", "create_index")
                    .grantedFields("grantedField1")
                    .query("{\"match_all\":{}}")
                    .allowRestrictedIndices(true)
                    .build() },
            randomFrom((RoleDescriptor.ApplicationResourcePrivileges[]) null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
            randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
            randomFrom((String[]) null, new String[0]),
            randomFrom((Map<String, Object>) null, Map.of()),
            Map.of()
        );
        RoleDescriptor roleDescriptor2 = new RoleDescriptor(
            "role_descriptor2",
            randomFrom((String[]) null, new String[0]),
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("na\"me", "*")
                    .privileges("manage_ilm")
                    .deniedFields("denied*")
                    .query("{\"match\": {\"category\": \"click\"}}")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("/@&~(\\.security.*)/")
                    .privileges("all", "cluster:a_wrong_*_one")
                    .build() },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("maps")
                    .resources("raster:*")
                    .privileges("coming", "up", "with", "random", "names", "is", "hard")
                    .build() },
            randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
            new String[] { "impersonated???" },
            randomFrom((Map<String, Object>) null, Map.of()),
            Map.of()
        );
        RoleDescriptor roleDescriptor3 = new RoleDescriptor(
            "role_descriptor3",
            randomFrom((String[]) null, new String[0]),
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
                    .build() },
            randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
            new String[] { "jack", "nich*", "//\"" },
            Map.of("some meta", 42),
            Map.of()
        );
        Map<String, Object> metaMap = new TreeMap<>();
        metaMap.put("?list", List.of("e1", "e2", "*"));
        metaMap.put("some other meta", Map.of("r", "t"));
        RoleDescriptor roleDescriptor4 = new RoleDescriptor(
            "role_descriptor4",
            new String[] { "manage_ml", "grant_api_key", "manage_rollup" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("/. ? + * | { } [ ] ( ) \" \\/", "*")
                    .privileges("read", "read_cross_cluster")
                    .grantedFields("almost", "all*")
                    .deniedFields("denied*")
                    .build() },
            randomFrom((RoleDescriptor.ApplicationResourcePrivileges[]) null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
            new ConfigurableClusterPrivilege[] { new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Set.of("a+b+|b+a+")) },
            new String[] { "//+a+\"[a]/" },
            metaMap,
            Map.of("ignored", 2)
        );
        RoleDescriptor roleDescriptor5 = new RoleDescriptor(
            "role_descriptor5",
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[0],
            randomFrom((RoleDescriptor.ApplicationResourcePrivileges[]) null, new RoleDescriptor.ApplicationResourcePrivileges[0]),
            new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(new LinkedHashSet<>(Arrays.asList("", "\""))),
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Set.of("\"")) },
            new String[] { "\"[a]/" },
            Map.of(),
            Map.of()
        );
        String keyName = randomAlphaOfLength(4);
        TimeValue expiration = randomFrom(new TimeValue(randomNonNegativeLong(), randomFrom(TimeUnit.values())), null);
        List<RoleDescriptor> allTestRoleDescriptors = List.of(
            nullRoleDescriptor,
            roleDescriptor1,
            roleDescriptor2,
            roleDescriptor3,
            roleDescriptor4,
            roleDescriptor5
        );
        List<RoleDescriptor> keyRoleDescriptors = randomSubsetOf(allTestRoleDescriptors);
        StringBuilder roleDescriptorsStringBuilder = new StringBuilder().append("\"role_descriptors\":[");
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
        final ApiKeyMetadataWithSerialization metadataWithSerialization = randomApiKeyMetadataWithSerialization();
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(keyName, keyRoleDescriptors, expiration);
        createApiKeyRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        createApiKeyRequest.setMetadata(metadataWithSerialization.metadata());
        auditTrail.accessGranted(requestId, authentication, CreateApiKeyAction.NAME, createApiKeyRequest, authorizationInfo);
        String expectedCreateKeyAuditEventString = String.format(
            Locale.ROOT,
            """
                "create":{"apikey":{"id":"%s","name":"%s","type":"rest","expiration":%s,%s%s}}\
                """,
            createApiKeyRequest.getId(),
            keyName,
            expiration != null ? "\"" + expiration + "\"" : "null",
            roleDescriptorsStringBuilder,
            createApiKeyRequest.getMetadata() == null || createApiKeyRequest.getMetadata().isEmpty()
                ? ""
                : Strings.format(",\"metadata\":%s", metadataWithSerialization.serialization())
        );
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
        assertMsg(generatedCreateKeyAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        final String keyId = randomAlphaOfLength(10);
        final var updateApiKeyRequest = new UpdateApiKeyRequest(
            keyId,
            randomBoolean() ? null : keyRoleDescriptors,
            metadataWithSerialization.metadata()
        );
        auditTrail.accessGranted(requestId, authentication, UpdateApiKeyAction.NAME, updateApiKeyRequest, authorizationInfo);
        final var expectedUpdateKeyAuditEventString = String.format(
            Locale.ROOT,
            """
                "change":{"apikey":{"id":"%s","type":"rest"%s%s}}\
                """,
            keyId,
            updateApiKeyRequest.getRoleDescriptors() == null ? "" : "," + roleDescriptorsStringBuilder,
            updateApiKeyRequest.getMetadata() == null ? "" : Strings.format(",\"metadata\":%s", metadataWithSerialization.serialization())
        );
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedUpdateKeyAuditEventString = output.get(1);
        assertThat(generatedUpdateKeyAuditEventString, containsString(expectedUpdateKeyAuditEventString));
        generatedUpdateKeyAuditEventString = generatedUpdateKeyAuditEventString.replace(", " + expectedUpdateKeyAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_apikey")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedUpdateKeyAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        final List<String> keyIds = randomList(1, 5, () -> randomAlphaOfLength(10));
        final var bulkUpdateApiKeyRequest = new BulkUpdateApiKeyRequest(
            keyIds,
            randomBoolean() ? null : keyRoleDescriptors,
            metadataWithSerialization.metadata()
        );
        auditTrail.accessGranted(requestId, authentication, BulkUpdateApiKeyAction.NAME, bulkUpdateApiKeyRequest, authorizationInfo);
        final var expectedBulkUpdateKeyAuditEventString = String.format(
            Locale.ROOT,
            """
                "change":{"apikeys":{"ids":[%s],"type":"rest"%s%s}}\
                """,
            bulkUpdateApiKeyRequest.getIds().stream().map(s -> Strings.format("\"%s\"", s)).collect(Collectors.joining(",")),
            bulkUpdateApiKeyRequest.getRoleDescriptors() == null ? "" : "," + roleDescriptorsStringBuilder,
            bulkUpdateApiKeyRequest.getMetadata() == null
                ? ""
                : Strings.format(",\"metadata\":%s", metadataWithSerialization.serialization())
        );
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedBulkUpdateKeyAuditEventString = output.get(1);
        assertThat(generatedBulkUpdateKeyAuditEventString, containsString(expectedBulkUpdateKeyAuditEventString));
        generatedBulkUpdateKeyAuditEventString = generatedBulkUpdateKeyAuditEventString.replace(
            ", " + expectedBulkUpdateKeyAuditEventString,
            ""
        );
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_apikeys")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedBulkUpdateKeyAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        grantApiKeyRequest.getGrant().setType(randomFrom(randomAlphaOfLength(8), null));
        grantApiKeyRequest.getGrant().setUsername(randomFrom(randomAlphaOfLength(8), null));
        grantApiKeyRequest.getGrant().setPassword(randomFrom(new SecureString("password not exposed"), null));
        grantApiKeyRequest.getGrant().setAccessToken(randomFrom(new SecureString("access token not exposed"), null));
        grantApiKeyRequest.getGrant().setRunAsUsername(randomFrom(randomAlphaOfLength(10), null));
        grantApiKeyRequest.setApiKeyRequest(createApiKeyRequest);
        auditTrail.accessGranted(requestId, authentication, GrantApiKeyAction.NAME, grantApiKeyRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedGrantKeyAuditEventString = output.get(1);
        StringBuilder grantKeyAuditEventStringBuilder = new StringBuilder().append("\"create\":{\"apikey\":{\"id\":\"")
            .append(grantApiKeyRequest.getApiKeyRequest().getId())
            .append("\",\"name\":\"")
            .append(keyName)
            .append("\",\"type\":\"rest\",\"expiration\":")
            .append(expiration != null ? "\"" + expiration + "\"" : "null")
            .append(",")
            .append(roleDescriptorsStringBuilder);
        if (grantApiKeyRequest.getApiKeyRequest().getMetadata() != null
            && grantApiKeyRequest.getApiKeyRequest().getMetadata().isEmpty() == false) {
            grantKeyAuditEventStringBuilder.append(",\"metadata\":").append(metadataWithSerialization.serialization());
        }
        grantKeyAuditEventStringBuilder.append("},\"grant\":{\"type\":");
        if (grantApiKeyRequest.getGrant().getType() != null) {
            grantKeyAuditEventStringBuilder.append("\"").append(grantApiKeyRequest.getGrant().getType()).append("\"");
        } else {
            grantKeyAuditEventStringBuilder.append("null");
        }
        if (grantApiKeyRequest.getGrant().getUsername() != null) {
            grantKeyAuditEventStringBuilder.append(",\"user\":{\"name\":\"")
                .append(grantApiKeyRequest.getGrant().getUsername())
                .append("\",\"has_password\":")
                .append(grantApiKeyRequest.getGrant().getPassword() != null)
                .append("}");
        }
        if (grantApiKeyRequest.getGrant().getAccessToken() != null) {
            grantKeyAuditEventStringBuilder.append(",\"has_access_token\":").append(true);
        }
        if (grantApiKeyRequest.getGrant().getRunAsUsername() != null) {
            grantKeyAuditEventStringBuilder.append(",\"run_as\":\"").append(grantApiKeyRequest.getGrant().getRunAsUsername()).append("\"");
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
        assertMsg(generatedGrantKeyAuditEventString, checkedFields.map());
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
        String expectedPutRoleAuditEventString = Strings.format("""
            "put":{"role":{"name":"%s","role_descriptor":%s}}\
            """, putRoleRequest.name(), auditedRolesMap.get(putRoleRequest.name()));
        assertThat(generatedPutRoleAuditEventString, containsString(expectedPutRoleAuditEventString));
        generatedPutRoleAuditEventString = generatedPutRoleAuditEventString.replace(", " + expectedPutRoleAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_role")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutRoleAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest();
        deleteRoleRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        deleteRoleRequest.name(putRoleRequest.name());
        auditTrail.accessGranted(requestId, authentication, DeleteRoleAction.NAME, deleteRoleRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteRoleAuditEventString = output.get(1);
        StringBuilder deleteRoleStringBuilder = new StringBuilder().append("\"delete\":{\"role\":{\"name\":");
        if (deleteRoleRequest.name() == null) {
            deleteRoleStringBuilder.append("null");
        } else {
            deleteRoleStringBuilder.append("\"").append(deleteRoleRequest.name()).append("\"");
        }
        deleteRoleStringBuilder.append("}}");
        String expectedDeleteRoleAuditEventString = deleteRoleStringBuilder.toString();
        assertThat(generatedDeleteRoleAuditEventString, containsString(expectedDeleteRoleAuditEventString));
        generatedDeleteRoleAuditEventString = generatedDeleteRoleAuditEventString.replace(", " + expectedDeleteRoleAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_role")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteRoleAuditEventString, checkedFields.map());
    }

    public void testSecurityConfigChangeEventForCrossClusterApiKeys() throws IOException {
        final CrossClusterApiKeyAccessWithSerialization accessWithSerialization = randomCrossClusterApiKeyAccessWithSerialization();
        final var roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(accessWithSerialization.access());

        // Create
        final String apiKeyName = randomAlphaOfLengthBetween(3, 8);
        final TimeValue expiration = randomFrom(TimeValue.timeValueHours(randomIntBetween(1, 99)), null);
        final ApiKeyMetadataWithSerialization metadataWithSerialization = randomApiKeyMetadataWithSerialization();
        final var createRequest = new CreateCrossClusterApiKeyRequest(
            apiKeyName,
            roleDescriptorBuilder,
            expiration,
            metadataWithSerialization.metadata()
        );

        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        auditTrail.accessGranted(requestId, authentication, CreateCrossClusterApiKeyAction.NAME, createRequest, authorizationInfo);

        final String expectedCreateAuditEventString = String.format(
            Locale.ROOT,
            """
                "create":{"apikey":{"id":"%s","name":"%s","type":"cross_cluster","expiration":%s,"role_descriptors":%s%s}}\
                """,
            createRequest.getId(),
            apiKeyName,
            expiration != null ? "\"" + expiration + "\"" : "null",
            accessWithSerialization.serialization(),
            createRequest.getMetadata() == null || createRequest.getMetadata().isEmpty()
                ? ""
                : Strings.format(",\"metadata\":%s", metadataWithSerialization.serialization())
        );

        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedCreateAuditEventString = output.get(1);
        assertThat(generatedCreateAuditEventString, containsString(expectedCreateAuditEventString));
        generatedCreateAuditEventString = generatedCreateAuditEventString.replace(", " + expectedCreateAuditEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "create_apikey")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedCreateAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // Update
        final CrossClusterApiKeyRoleDescriptorBuilder updateAccess = randomFrom(roleDescriptorBuilder, null);
        final ApiKeyMetadataWithSerialization updateMetadataWithSerialization;
        if (null == updateAccess) {
            updateMetadataWithSerialization = randomValueOtherThanMany(
                m -> m.metadata == null,
                this::randomApiKeyMetadataWithSerialization
            );
        } else {
            updateMetadataWithSerialization = randomApiKeyMetadataWithSerialization();
        }

        final var updateRequest = new UpdateCrossClusterApiKeyRequest(
            createRequest.getId(),
            updateAccess,
            updateMetadataWithSerialization.metadata()
        );
        auditTrail.accessGranted(requestId, authentication, UpdateCrossClusterApiKeyAction.NAME, updateRequest, authorizationInfo);

        final String expectedUpdateAuditEventString = String.format(
            Locale.ROOT,
            """
                "change":{"apikey":{"id":"%s","type":"cross_cluster"%s%s}}\
                """,
            createRequest.getId(),
            updateAccess == null ? "" : ",\"role_descriptors\":" + accessWithSerialization.serialization(),
            updateRequest.getMetadata() == null ? "" : Strings.format(",\"metadata\":%s", updateMetadataWithSerialization.serialization())
        );

        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedUpdateAuditEventString = output.get(1);
        assertThat(generatedUpdateAuditEventString, containsString(expectedUpdateAuditEventString));
        generatedUpdateAuditEventString = generatedUpdateAuditEventString.replace(", " + expectedUpdateAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_apikey")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedUpdateAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
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
            randomFrom(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(8)), null)
        );

        auditTrail.accessGranted(requestId, authentication, InvalidateApiKeyAction.NAME, invalidateApiKeyRequest, authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedInvalidateKeyAuditEventString = output.get(1);
        StringBuilder invalidateKeyEventStringBuilder = new StringBuilder().append("""
            "invalidate":{"apikeys":{""");
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
            invalidateKeyEventStringBuilder.append("""
                ,"user":{"name":""");
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
        generatedInvalidateKeyAuditEventString = generatedInvalidateKeyAuditEventString.replace(
            ", " + expectedInvalidateKeyEventString,
            ""
        );
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "invalidate_apikeys")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedInvalidateKeyAuditEventString, checkedFields.map());
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
            serializedMetadata = """
                ,"metadata":{"ans":42,"test":true}""";
        } else {
            metadata.put("ans", List.of(42, true));
            metadata.put("other", Map.of("42", true));
            serializedMetadata = """
                ,"metadata":{"ans":[42,true],"other":{"42":true}}""";
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
        StringBuilder putPrivilegesAuditEventStringBuilder = new StringBuilder().append("""
            "put":{"privileges":[""");
        if (false == putPrivilegesRequest.getPrivileges().isEmpty()) {
            for (ApplicationPrivilegeDescriptor appPriv : putPrivilegesRequest.getPrivileges()) {
                putPrivilegesAuditEventStringBuilder.append("{\"application\":\"")
                    .append(appPriv.getApplication())
                    .append("\"")
                    .append(",\"name\":\"")
                    .append(appPriv.getName())
                    .append("\"")
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
        generatedPutPrivilegesAuditEventString = generatedPutPrivilegesAuditEventString.replace(
            ", " + expectedPutPrivilegesEventString,
            ""
        );
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_privileges")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutPrivilegesAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        DeletePrivilegesRequest deletePrivilegesRequest = new DeletePrivilegesRequest(
            randomFrom(randomAlphaOfLength(8), null),
            generateRandomStringArray(4, 4, true)
        );
        deletePrivilegesRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        auditTrail.accessGranted(requestId, authentication, DeletePrivilegesAction.NAME, deletePrivilegesRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeletePrivilegesAuditEventString = output.get(1);
        StringBuilder deletePrivilegesAuditEventStringBuilder = new StringBuilder().append("""
            "delete":{"privileges":{"application":""");
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
        generatedDeletePrivilegesAuditEventString = generatedDeletePrivilegesAuditEventString.replace(
            ", " + expectedDeletePrivilegesEventString,
            ""
        );
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_privileges")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeletePrivilegesAuditEventString, checkedFields.map());
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
        putRoleMappingRequest.setRoleTemplates(
            Arrays.asList(
                randomArray(
                    4,
                    TemplateRoleName[]::new,
                    () -> new TemplateRoleName(
                        new BytesArray(randomAlphaOfLengthBetween(0, 8)),
                        randomFrom(TemplateRoleName.Format.values())
                    )
                )
            )
        );
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
        StringBuilder putRoleMappingAuditEventStringBuilder = new StringBuilder().append("""
            "put":{"role_mapping":{"name":""");
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
            putRoleMappingAuditEventStringBuilder.append("""
                ,"rules":{"mock":"A mock role mapper expression"}""");
        } else {
            putRoleMappingAuditEventStringBuilder.append("""
                ,"rules":null""");
        }
        putRoleMappingAuditEventStringBuilder.append("""
            ,"enabled":""").append(putRoleMappingRequest.isEnabled());
        if (hasMetadata) {
            putRoleMappingAuditEventStringBuilder.append("""
                ,"metadata":{"list":["42",13],"smth":42}}}""");
        } else {
            putRoleMappingAuditEventStringBuilder.append("}}");
        }
        String expectedPutRoleMappingAuditEventString = putRoleMappingAuditEventStringBuilder.toString();
        assertThat(generatedPutRoleMappingAuditEventString, containsString(expectedPutRoleMappingAuditEventString));
        generatedPutRoleMappingAuditEventString = generatedPutRoleMappingAuditEventString.replace(
            ", " + expectedPutRoleMappingAuditEventString,
            ""
        );
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "put_role_mapping")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedPutRoleMappingAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        DeleteRoleMappingRequest deleteRoleMappingRequest = new DeleteRoleMappingRequest();
        deleteRoleMappingRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        deleteRoleMappingRequest.setName(putRoleMappingRequest.getName());
        auditTrail.accessGranted(requestId, authentication, DeleteRoleMappingAction.NAME, deleteRoleMappingRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteRoleMappingAuditEventString = output.get(1);
        StringBuilder deleteRoleMappingStringBuilder = new StringBuilder().append("""
            "delete":{"role_mapping":{"name":""");
        if (deleteRoleMappingRequest.getName() == null) {
            deleteRoleMappingStringBuilder.append("null");
        } else {
            deleteRoleMappingStringBuilder.append("\"").append(deleteRoleMappingRequest.getName()).append("\"");
        }
        deleteRoleMappingStringBuilder.append("}}");
        String expectedDeleteRoleMappingAuditEventString = deleteRoleMappingStringBuilder.toString();
        assertThat(generatedDeleteRoleMappingAuditEventString, containsString(expectedDeleteRoleMappingAuditEventString));
        generatedDeleteRoleMappingAuditEventString = generatedDeleteRoleMappingAuditEventString.replace(
            ", " + expectedDeleteRoleMappingAuditEventString,
            ""
        );
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_role_mapping")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteRoleMappingAuditEventString, checkedFields.map());
    }

    public void testSecurityConfigChangeEventFormattingForUsers() throws IOException {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();

        PutUserRequest putUserRequest = new PutUserRequest();
        String username = randomFrom(
            randomAlphaOfLength(3),
            customAnonymousUsername,
            AnonymousUser.DEFAULT_ANONYMOUS_USERNAME,
            UsernamesField.ELASTIC_NAME,
            UsernamesField.KIBANA_NAME
        );
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

        StringBuilder putUserAuditEventStringBuilder = new StringBuilder().append("""
            "put":{"user":{"name":""")
            .append("\"")
            .append(putUserRequest.username())
            .append("\"")
            .append(",\"enabled\":")
            .append(putUserRequest.enabled())
            .append(",\"roles\":");
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
            putUserAuditEventStringBuilder.append("""
                ,"metadata":{"list":["42",13],"smth":42}}}""");
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
        assertMsg(generatedPutUserAuditEventString, checkedFields.map());
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
        String expectedEnableUserAuditEventString = Strings.format("""
            "change":{"enable":{"user":{"name":"%s"}}}\
            """, username);
        assertThat(generatedEnableUserAuditEventString, containsString(expectedEnableUserAuditEventString));
        generatedEnableUserAuditEventString = generatedEnableUserAuditEventString.replace(", " + expectedEnableUserAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_enable_user")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedEnableUserAuditEventString, checkedFields.map());
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
        String expectedDisableUserAuditEventString = Strings.format("""
            "change":{"disable":{"user":{"name":"%s"}}}\
            """, username);
        assertThat(generatedDisableUserAuditEventString, containsString(expectedDisableUserAuditEventString));
        generatedDisableUserAuditEventString = generatedDisableUserAuditEventString.replace(", " + expectedDisableUserAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_disable_user")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDisableUserAuditEventString, checkedFields.map());
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
        String expectedChangePasswordAuditEventString = Strings.format("""
            "change":{"password":{"user":{"name":"%s"}}}\
            """, username);
        assertThat(generatedChangePasswordAuditEventString, containsString(expectedChangePasswordAuditEventString));
        generatedChangePasswordAuditEventString = generatedChangePasswordAuditEventString.replace(
            ", " + expectedChangePasswordAuditEventString,
            ""
        );
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "change_password")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedChangePasswordAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        DeleteUserRequest deleteUserRequest = new DeleteUserRequest();
        deleteUserRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        deleteUserRequest.username(username);
        auditTrail.accessGranted(requestId, authentication, DeleteUserAction.NAME, deleteUserRequest, authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteUserAuditEventString = output.get(1);
        String expectedDeleteUserAuditEventString = Strings.format("""
            "delete":{"user":{"name":"%s"}}\
            """, username);
        assertThat(generatedDeleteUserAuditEventString, containsString(expectedDeleteUserAuditEventString));
        generatedDeleteUserAuditEventString = generatedDeleteUserAuditEventString.replace(", " + expectedDeleteUserAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_user")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteUserAuditEventString, checkedFields.map());
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
            namespace,
            serviceName,
            tokenName
        );

        auditTrail.accessGranted(
            requestId,
            authentication,
            CreateServiceAccountTokenAction.NAME,
            createServiceAccountTokenRequest,
            authorizationInfo
        );
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedCreateServiceAccountTokenAuditEventString = output.get(1);

        final String expectedCreateServiceAccountTokenAuditEventString = Strings.format("""
            "create":{"service_token":{"namespace":"%s","service":"%s","name":"%s"}}""", namespace, serviceName, tokenName);
        assertThat(generatedCreateServiceAccountTokenAuditEventString, containsString(expectedCreateServiceAccountTokenAuditEventString));
        generatedCreateServiceAccountTokenAuditEventString = generatedCreateServiceAccountTokenAuditEventString.replace(
            ", " + expectedCreateServiceAccountTokenAuditEventString,
            ""
        );
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "create_service_token")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedCreateServiceAccountTokenAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest = new DeleteServiceAccountTokenRequest(
            namespace,
            serviceName,
            tokenName
        );

        auditTrail.accessGranted(
            requestId,
            authentication,
            DeleteServiceAccountTokenAction.NAME,
            deleteServiceAccountTokenRequest,
            authorizationInfo
        );
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        String generatedDeleteServiceAccountTokenAuditEventString = output.get(1);

        final String expectedDeleteServiceAccountTokenAuditEventString = Strings.format("""
            "delete":{"service_token":{"namespace":"%s","service":"%s","name":"%s"}}""", namespace, serviceName, tokenName);
        assertThat(generatedDeleteServiceAccountTokenAuditEventString, containsString(expectedDeleteServiceAccountTokenAuditEventString));
        generatedDeleteServiceAccountTokenAuditEventString = generatedDeleteServiceAccountTokenAuditEventString.replace(
            ", " + expectedDeleteServiceAccountTokenAuditEventString,
            ""
        );
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "delete_service_token")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedDeleteServiceAccountTokenAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
    }

    public void testSecurityConfigChangeEventFormattingForUserProfile() {
        final String requestId = randomRequestId();
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Map.of(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final Authentication authentication = createAuthentication();
        final List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);

        // Activate profile
        final ActivateProfileRequest activateProfileRequest = new ActivateProfileRequest();
        activateProfileRequest.getGrant().setType(randomFrom(randomAlphaOfLength(8), null));
        activateProfileRequest.getGrant().setUsername(randomFrom(randomAlphaOfLength(8), null));
        activateProfileRequest.getGrant().setPassword(randomFrom(new SecureString("password not exposed"), null));
        activateProfileRequest.getGrant().setAccessToken(randomFrom(new SecureString("access token not exposed"), null));
        auditTrail.accessGranted(requestId, authentication, ActivateProfileAction.NAME, activateProfileRequest, authorizationInfo);
        assertThat(output.size(), is(2));
        String generatedActivateAuditEventString = output.get(1);

        final StringBuilder activateAuditEventStringBuilder = new StringBuilder().append("\"put\":{\"grant\":{\"type\":");
        if (activateProfileRequest.getGrant().getType() != null) {
            activateAuditEventStringBuilder.append("\"").append(activateProfileRequest.getGrant().getType()).append("\"");
        } else {
            activateAuditEventStringBuilder.append("null");
        }
        if (activateProfileRequest.getGrant().getUsername() != null) {
            activateAuditEventStringBuilder.append(",\"user\":{\"name\":\"")
                .append(activateProfileRequest.getGrant().getUsername())
                .append("\",\"has_password\":")
                .append(activateProfileRequest.getGrant().getPassword() != null)
                .append("}");
        }
        if (activateProfileRequest.getGrant().getAccessToken() != null) {
            activateAuditEventStringBuilder.append(",\"has_access_token\":").append(true);
        }
        activateAuditEventStringBuilder.append("}}");
        String expectedActivateAuditEventString = activateAuditEventStringBuilder.toString();
        assertThat(generatedActivateAuditEventString, containsString(expectedActivateAuditEventString));
        generatedActivateAuditEventString = generatedActivateAuditEventString.replace(", " + expectedActivateAuditEventString, "");
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "activate_user_profile")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedActivateAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // Update profile data
        final UpdateProfileDataRequest updateProfileDataRequest = new UpdateProfileDataRequest(
            randomAlphaOfLength(20),
            Map.of("space", "production"),
            Map.of("theme", "default"),
            randomLongBetween(-1, Long.MAX_VALUE),
            randomLongBetween(-1, Long.MAX_VALUE),
            WriteRequest.RefreshPolicy.WAIT_UNTIL
        );
        auditTrail.accessGranted(requestId, authentication, UpdateProfileDataAction.NAME, updateProfileDataRequest, authorizationInfo);
        assertThat(output.size(), is(2));
        String generatedUpdateAuditEventString = output.get(1);
        final String expectedUpdateAuditEventString = Strings.format("""
            "put":{"uid":"%s","labels":{"space":"production"},"data":{"theme":"default"}}""", updateProfileDataRequest.getUid());
        assertThat(generatedUpdateAuditEventString, containsString(expectedUpdateAuditEventString));
        generatedUpdateAuditEventString = generatedUpdateAuditEventString.replace(", " + expectedUpdateAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "update_user_profile_data")
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedUpdateAuditEventString, checkedFields.map());
        // clear log
        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // SetEnabled user profile
        final SetProfileEnabledRequest setProfileEnabledRequest = new SetProfileEnabledRequest(
            randomAlphaOfLength(20),
            randomBoolean(),
            WriteRequest.RefreshPolicy.WAIT_UNTIL
        );
        auditTrail.accessGranted(requestId, authentication, SetProfileEnabledAction.NAME, setProfileEnabledRequest, authorizationInfo);
        assertThat(output.size(), is(2));
        String generatedSetEnabledAuditEventString = output.get(1);
        final String expectedSetEnabledAuditEventString = String.format(
            Locale.ROOT,
            """
                "change":{"%s":{"uid":"%s"}}""",
            setProfileEnabledRequest.isEnabled() ? "enable" : "disable",
            setProfileEnabledRequest.getUid()
        );
        assertThat(generatedSetEnabledAuditEventString, containsString(expectedSetEnabledAuditEventString));
        generatedSetEnabledAuditEventString = generatedSetEnabledAuditEventString.replace(", " + expectedSetEnabledAuditEventString, "");
        checkedFields = new MapBuilder<>(commonFields);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME);
        checkedFields.remove(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME);
        Object[] args = new Object[] { setProfileEnabledRequest.isEnabled() ? "enable" : "disable" };
        checkedFields.put("type", "audit")
            .put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "security_config_change")
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, Strings.format("change_%s_user_profile", args))
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        assertMsg(generatedSetEnabledAuditEventString, checkedFields.map());
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied").build()
        );
        auditTrail.anonymousAccessDenied(requestId, "_action", request);
        assertEmptyLog(logger);
    }

    public void testAnonymousAccessDeniedRest() throws Exception {
        final InetSocketAddress address = new InetSocketAddress(
            forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
            randomIntBetween(9200, 9300)
        );
        final Tuple<Channel, RestRequest> tuple = prepareRestContent("_uri", address);
        final RestRequest request = tuple.v2();
        RemoteHostHeader.process(tuple.v1(), threadContext);

        final String requestId = randomRequestId();
        auditTrail.anonymousAccessDenied(requestId, request.getHttpRequest());
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "anonymous_access_denied")
            .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
            .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
            .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "anonymous_access_denied").build()
        );
        auditTrail.anonymousAccessDenied(requestId, request.getHttpRequest());
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build()
        );
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build()
        );
        auditTrail.authenticationFailed(requestId, "_action", request);
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("foo", "bar");
        }
        final InetSocketAddress address = new InetSocketAddress(
            forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
            randomIntBetween(9200, 9300)
        );
        final Tuple<Channel, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final RestRequest request = tuple.v2();
        final AuthenticationToken authToken = createAuthenticationToken();
        RemoteHostHeader.process(tuple.v1(), threadContext);

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, authToken, request.getHttpRequest());
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
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "foo=bar");
        }
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build()
        );
        auditTrail.authenticationFailed(requestId, createAuthenticationToken(), request.getHttpRequest());
        assertEmptyLog(logger);
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("bar", "baz");
        }
        final InetSocketAddress address = new InetSocketAddress(
            forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
            randomIntBetween(9200, 9300)
        );
        final Tuple<Channel, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final RestRequest request = tuple.v2();
        RemoteHostHeader.process(tuple.v1(), threadContext);

        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, request.getHttpRequest());
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_failed")
            .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
            .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
            .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "bar=baz");
        }
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "authentication_failed").build()
        );
        auditTrail.authenticationFailed(requestId, request.getHttpRequest());
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
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "realm_authentication_failed").build()
        );
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());
    }

    public void testAuthenticationFailedRealmRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("_param", "baz");
        }
        final InetSocketAddress address = new InetSocketAddress(
            forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
            randomIntBetween(9200, 9300)
        );
        final Tuple<Channel, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final RestRequest request = tuple.v2();
        final AuthenticationToken authToken = mockToken();
        final String realm = randomAlphaOfLengthBetween(1, 6);
        RemoteHostHeader.process(tuple.v1(), threadContext);
        final String requestId = randomRequestId();
        auditTrail.authenticationFailed(requestId, realm, authToken, request.getHttpRequest());
        assertEmptyLog(logger);

        // test enabled
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "realm_authentication_failed").build()
        );
        auditTrail.authenticationFailed(requestId, realm, authToken, request.getHttpRequest());
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
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "_param=baz");
        }
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthenticationAndMaybeWithRunAs(authentication);
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "access_granted").build());
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
        Tuple<String, TransportRequest> actionAndRequest = randomFrom(
            new Tuple<>(PutUserAction.NAME, new PutUserRequest()),
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
            new Tuple<>(DeleteServiceAccountTokenAction.NAME, new DeleteServiceAccountTokenRequest(namespace, serviceName, tokenName)),
            new Tuple<>(ActivateProfileAction.NAME, new ActivateProfileRequest()),
            new Tuple<>(
                UpdateProfileDataAction.NAME,
                new UpdateProfileDataRequest(randomAlphaOfLength(20), Map.of(), Map.of(), -1, -1, WriteRequest.RefreshPolicy.WAIT_UNTIL)
            ),
            new Tuple<>(
                SetProfileEnabledAction.NAME,
                new SetProfileEnabledRequest(randomAlphaOfLength(20), randomBoolean(), WriteRequest.RefreshPolicy.WAIT_UNTIL)
            ),
            new Tuple<>(UpdateApiKeyAction.NAME, UpdateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(10)))
        );
        auditTrail.accessGranted(requestId, authentication, actionAndRequest.v1(), actionAndRequest.v2(), authorizationInfo);
        List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(2));
        assertThat(output.get(1), containsString("security_config_change"));
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "security_config_change").build()
        );
        auditTrail.accessGranted(requestId, authentication, actionAndRequest.v1(), actionAndRequest.v2(), authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), not(containsString("security_config_change")));
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder()
                .put(settings)
                .put("xpack.security.audit.logfile.events.include", "security_config_change")
                .put("xpack.security.audit.logfile.events.exclude", "access_granted")
                .build()
        );
        auditTrail.accessGranted(requestId, authentication, actionAndRequest.v1(), actionAndRequest.v2(), authorizationInfo);
        output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), containsString("security_config_change"));
    }

    public void testSystemAccessGranted() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final InternalUser systemUser = AuthenticationTestHelper.randomInternalUser();
        final Authentication authentication = AuthenticationTestHelper.builder().internal(systemUser).build();
        final String requestId = randomRequestId();

        auditTrail.accessGranted(requestId, authentication, "_action", request, authorizationInfo);
        // system user
        assertEmptyLog(logger);
        auditTrail.explicitIndexAccessEvent(
            requestId,
            randomFrom(AuditLevel.ACCESS_GRANTED, AuditLevel.SYSTEM_ACCESS_GRANTED),
            authentication,
            "_action",
            randomFrom(randomAlphaOfLengthBetween(1, 4), null),
            BulkItemRequest.class.getName(),
            request.remoteAddress(),
            authorizationInfo
        );
        // system user
        assertEmptyLog(logger);

        // enable system user for access granted events
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "system_access_granted").build()
        );

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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());
        clearLog();

        String index = randomFrom(randomAlphaOfLengthBetween(1, 4), null);
        auditTrail.explicitIndexAccessEvent(
            requestId,
            randomFrom(AuditLevel.ACCESS_GRANTED, AuditLevel.SYSTEM_ACCESS_GRANTED),
            authentication,
            "_action",
            index,
            BulkItemRequest.class.getName(),
            request.remoteAddress(),
            authorizationInfo
        );

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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        if (index != null) {
            checkedArrayFields.put(LoggingAuditTrail.INDICES_FIELD_NAME, new String[] { index });
        }
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());
    }

    public void testAccessGrantedInternalSystemAction() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final InternalUser systemUser = AuthenticationTestHelper.randomInternalUser();
        final Authentication authentication = AuthenticationTestHelper.builder().internal(systemUser).build();
        final String requestId = randomRequestId();
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        assertEmptyLog(logger);

        // test enabled
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "system_access_granted").build()
        );
        auditTrail.accessGranted(requestId, authentication, "internal:_action", request, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "access_granted")
            .put(LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME, authentication.getAuthenticationType().toString())
            .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, systemUser.principal())
            .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authentication.getEffectiveSubject().getRealm().getName())
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "internal:_action")
            .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthenticationAndMaybeWithRunAs(authentication);
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "access_granted").build());
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
            checkedFields.put(
                LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME,
                (String) authentication.getAuthenticatingSubject().getMetadata().get(TOKEN_NAME_FIELD)
            )
                .put(
                    LoggingAuditTrail.SERVICE_TOKEN_TYPE_FIELD_NAME,
                    ServiceAccountSettings.REALM_TYPE
                        + "_"
                        + authentication.getAuthenticatingSubject().getMetadata().get(TOKEN_SOURCE_FIELD)
                );
        }
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        authentication(authentication, checkedFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthenticationAndMaybeWithRunAs(authentication);
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "access_denied").build());
        auditTrail.accessDenied(requestId, authentication, "_action", request, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testTamperedRequestRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("_param", "baz");
        }
        final InetSocketAddress address = new InetSocketAddress(
            forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
            randomIntBetween(9200, 9300)
        );
        final Tuple<Channel, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final RestRequest request = tuple.v2();
        RemoteHostHeader.process(tuple.v1(), threadContext);
        final String requestId = randomRequestId();
        auditTrail.tamperedRequest(requestId, request.getHttpRequest());
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "tampered_request")
            .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
            .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
            .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "_param=baz");
        }
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "tampered_request").build()
        );
        auditTrail.tamperedRequest(requestId, request.getHttpRequest());
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "tampered_request").build()
        );
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthenticationAndMaybeWithRunAs(authentication);
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "tampered_request").build()
        );
        auditTrail.tamperedRequest(requestId, authentication, "_action", request);
        assertEmptyLog(logger);
    }

    public void testConnectionDenied() throws Exception {
        final InetSocketAddress inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), randomIntBetween(0, 65535));
        final SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        final String profile = randomBoolean() ? IPFilter.HTTP_PROFILE_NAME : randomAlphaOfLengthBetween(1, 6);

        auditTrail.connectionDenied(inetAddress, profile, rule);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.IP_FILTER_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "connection_denied")
            .put(
                LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME,
                IPFilter.HTTP_PROFILE_NAME.equals(profile)
                    ? LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE
                    : LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE
            )
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
            .put(LoggingAuditTrail.TRANSPORT_PROFILE_FIELD_NAME, profile)
            .put(LoggingAuditTrail.RULE_FIELD_NAME, "deny _all");
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "connection_denied").build()
        );
        auditTrail.connectionDenied(inetAddress, profile, rule);
        assertEmptyLog(logger);
    }

    public void testConnectionGranted() throws Exception {
        final InetSocketAddress inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), randomIntBetween(0, 65535));
        final SecurityIpFilterRule rule = IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        final String profile = randomBoolean() ? IPFilter.HTTP_PROFILE_NAME : randomAlphaOfLengthBetween(1, 6);

        auditTrail.connectionGranted(inetAddress, profile, rule);
        assertEmptyLog(logger);

        // test enabled
        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "connection_granted").build()
        );
        auditTrail.connectionGranted(inetAddress, profile, rule);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.IP_FILTER_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "connection_granted")
            .put(
                LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME,
                IPFilter.HTTP_PROFILE_NAME.equals(profile)
                    ? LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE
                    : LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE
            )
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(inetAddress))
            .put(LoggingAuditTrail.TRANSPORT_PROFILE_FIELD_NAME, profile)
            .put(LoggingAuditTrail.RULE_FIELD_NAME, "allow default:accept_all");
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());
    }

    public void testRunAsGranted() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final RealmRef authRealmRef = AuthenticationTestHelper.randomRealmRef();
        final RealmRef lookupRealmRef = AuthenticationTestHelper.randomRealmRef();
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("_username", "r1"))
            .realmRef(authRealmRef)
            .runAs()
            .user(new User("running as", "r2"))
            .realmRef(lookupRealmRef)
            .build();
        final String requestId = randomRequestId();

        auditTrail.runAsGranted(requestId, authentication, "_action", request, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "run_as_granted")
            .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username")
            .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authRealmRef.getName())
            .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME, "running as")
            .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME, lookupRealmRef.getName())
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
            .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        if (authRealmRef.getDomain() != null) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_DOMAIN_FIELD_NAME, authRealmRef.getDomain().name());
        }
        if (lookupRealmRef.getDomain() != null) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_RUN_AS_DOMAIN_FIELD_NAME, lookupRealmRef.getDomain().name());
        }
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "run_as_granted").build());
        auditTrail.runAsGranted(requestId, authentication, "_action", request, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testRunAsDenied() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String[] expectedRoles = randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4));
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(PRINCIPAL_ROLES_FIELD_NAME, expectedRoles);
        final RealmRef authRealmRef = AuthenticationTestHelper.randomRealmRef();
        final RealmRef lookupRealmRef = AuthenticationTestHelper.randomRealmRef();
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("_username", "r1"))
            .realmRef(authRealmRef)
            .runAs()
            .user(new User("running as", "r2"))
            .realmRef(lookupRealmRef)
            .build();
        final String requestId = randomRequestId();

        auditTrail.runAsDenied(requestId, authentication, "_action", request, authorizationInfo);
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "run_as_denied")
            .put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, "_username")
            .put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authRealmRef.getName())
            .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME, "running as")
            .put(LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME, lookupRealmRef.getName())
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
            .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        if (authRealmRef.getDomain() != null) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_DOMAIN_FIELD_NAME, authRealmRef.getDomain().name());
        }
        if (lookupRealmRef.getDomain() != null) {
            checkedFields.put(LoggingAuditTrail.PRINCIPAL_RUN_AS_DOMAIN_FIELD_NAME, lookupRealmRef.getDomain().name());
        }
        checkedArrayFields.put(PRINCIPAL_ROLES_FIELD_NAME, (String[]) authorizationInfo.asMap().get(PRINCIPAL_ROLES_FIELD_NAME));
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        // test disabled
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        updateLoggerSettings(Settings.builder().put(settings).put("xpack.security.audit.logfile.events.exclude", "run_as_denied").build());
        auditTrail.runAsDenied(requestId, authentication, "_action", request, authorizationInfo);
        assertEmptyLog(logger);
    }

    public void testAuthenticationSuccessRest() throws Exception {
        final Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("foo", "bar");
            params.put("evac", "true");
        }
        final InetSocketAddress address = new InetSocketAddress(
            forge("_hostname", randomBoolean() ? "127.0.0.1" : "::1"),
            randomIntBetween(9200, 9300)
        );
        final Tuple<Channel, RestRequest> tuple = prepareRestContent("_uri", address, params);
        final RestRequest request = tuple.v2();
        String requestId = AuditUtil.generateRequestId(threadContext);
        MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        Authentication authentication = createAuthentication();
        authentication.writeToContext(threadContext);
        RemoteHostHeader.process(tuple.v1(), threadContext);

        // event by default disabled
        auditTrail.authenticationSuccess(request);
        assertEmptyLog(logger);

        updateLoggerSettings(
            Settings.builder().put(this.settings).put("xpack.security.audit.logfile.events.include", "authentication_success").build()
        );
        auditTrail.authenticationSuccess(request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
            .put(LoggingAuditTrail.REALM_FIELD_NAME, authentication.getAuthenticatingSubject().getRealm().getName())
            .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
            .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
            .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(request.content())) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, request.content().utf8ToString());
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "foo=bar&evac=true");
        }
        authentication(authentication, checkedFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        threadContext.stashContext();

        // audit for authn with API Key
        requestId = AuditUtil.generateRequestId(threadContext);
        authentication = createApiKeyAuthenticationAndMaybeWithRunAs(authentication);
        authentication.writeToContext(threadContext);
        checkedFields = new MapBuilder<>(commonFields);
        RemoteHostHeader.process(tuple.v1(), threadContext);
        auditTrail.authenticationSuccess(request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
            .put(LoggingAuditTrail.REALM_FIELD_NAME, "_es_api_key")
            .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
            .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
            .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(request.content())) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, request.getHttpRequest().content().utf8ToString());
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "foo=bar&evac=true");
        }
        authentication(authentication, checkedFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        threadContext.stashContext();

        // authentication success but run-as user does not exist
        requestId = AuditUtil.generateRequestId(threadContext);
        authentication = AuthenticationTestHelper.builder().realm().build(false).runAs(new User(randomAlphaOfLengthBetween(3, 8)), null);
        authentication.writeToContext(threadContext);
        checkedFields = new MapBuilder<>(commonFields);
        RemoteHostHeader.process(tuple.v1(), threadContext);
        auditTrail.authenticationSuccess(request);
        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
            .put(LoggingAuditTrail.REALM_FIELD_NAME, authentication.getAuthenticatingSubject().getRealm().getName())
            .put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address))
            .put(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.method().toString())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId)
            .put(LoggingAuditTrail.URL_PATH_FIELD_NAME, "_uri");
        if (includeRequestBody && Strings.hasLength(request.content().utf8ToString())) {
            checkedFields.put(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, request.content().utf8ToString());
        }
        if (params.isEmpty() == false) {
            checkedFields.put(LoggingAuditTrail.URL_QUERY_FIELD_NAME, "foo=bar&evac=true");
        }
        authentication(authentication, checkedFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map());
        CapturingLogger.output(logger.getName(), Level.INFO).clear();
        threadContext.stashContext();
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

        updateLoggerSettings(
            Settings.builder().put(this.settings).put("xpack.security.audit.logfile.events.include", "authentication_success").build()
        );
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());

        CapturingLogger.output(logger.getName(), Level.INFO).clear();

        // audit for authn with API Key
        authentication = createApiKeyAuthenticationAndMaybeWithRunAs(authentication);
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
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(logger, checkedFields.map(), checkedArrayFields.map());
    }

    public void testCrossClusterAccessAuthenticationSuccessTransport() throws Exception {
        final TransportRequest request = randomBoolean() ? new MockRequest(threadContext) : new MockIndicesRequest(threadContext);
        final String requestId = randomRequestId();
        final Authentication remoteAuthentication = randomFrom(
            AuthenticationTestHelper.builder().realm(false),
            AuthenticationTestHelper.builder().internal(InternalUsers.CROSS_CLUSTER_ACCESS_USER),
            AuthenticationTestHelper.builder().serviceAccount(),
            AuthenticationTestHelper.builder().apiKey().metadata(Map.of(AuthenticationField.API_KEY_NAME_KEY, randomAlphaOfLength(42)))
        ).build(false);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(
                randomAlphaOfLength(42),
                new CrossClusterAccessSubjectInfo(remoteAuthentication, RoleDescriptorsIntersection.EMPTY).cleanAndValidate()
            )
            .build();
        final MapBuilder<String, String> checkedFields = new MapBuilder<>(commonFields);
        final MapBuilder<String, String[]> checkedArrayFields = new MapBuilder<>();
        final MapBuilder<String, String> checkedLiteralFields = new MapBuilder<>();

        updateLoggerSettings(
            Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "authentication_success").build()
        );
        auditTrail.authenticationSuccess(requestId, authentication, "_action", request);

        checkedFields.put(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
            .put(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME, "authentication_success")
            .put(LoggingAuditTrail.ACTION_FIELD_NAME, "_action")
            .put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, request.getClass().getSimpleName())
            .put(LoggingAuditTrail.REQUEST_ID_FIELD_NAME, requestId);
        crossClusterAccessAuthentication(authentication, remoteAuthentication, checkedFields, checkedLiteralFields);
        restOrTransportOrigin(request, threadContext, checkedFields);
        indicesRequest(request, checkedFields, checkedArrayFields);
        opaqueId(threadContext, checkedFields);
        traceId(threadContext, checkedFields);
        forwardedFor(threadContext, checkedFields);
        assertMsg(singleLogLine(logger), checkedFields.map(), checkedArrayFields.map(), checkedLiteralFields.map());
    }

    public void testRequestsWithoutIndices() throws Exception {
        settings = Settings.builder().put(settings).put("xpack.security.audit.logfile.events.include", "_all").build();
        auditTrail = new LoggingAuditTrail(settings, clusterService, logger, threadContext);
        final AuthorizationInfo authorizationInfo = () -> Collections.singletonMap(
            PRINCIPAL_ROLES_FIELD_NAME,
            new String[] { randomAlphaOfLengthBetween(1, 6) }
        );
        final String realm = randomAlphaOfLengthBetween(1, 6);
        // transport messages without indices
        final TransportRequest[] requests = new TransportRequest[] {
            new MockRequest(threadContext),
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
            auditTrail.accessGranted(
                "_req_id",
                randomBoolean() ? createAuthentication() : createApiKeyAuthenticationAndMaybeWithRunAs(createAuthentication()),
                "_action",
                request,
                authorizationInfo
            );
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.accessDenied(
                "_req_id",
                randomBoolean() ? createAuthentication() : createApiKeyAuthenticationAndMaybeWithRunAs(createAuthentication()),
                "_action",
                request,
                authorizationInfo
            );
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.tamperedRequest("_req_id", "_action", request);
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.tamperedRequest(
                "_req_id",
                randomBoolean() ? createAuthentication() : createApiKeyAuthenticationAndMaybeWithRunAs(createAuthentication()),
                "_action",
                request
            );
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.runAsGranted(
                "_req_id",
                randomBoolean() ? createAuthentication() : createApiKeyAuthenticationAndMaybeWithRunAs(createAuthentication()),
                "_action",
                request,
                authorizationInfo
            );
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.runAsDenied(
                "_req_id",
                randomBoolean() ? createAuthentication() : createApiKeyAuthenticationAndMaybeWithRunAs(createAuthentication()),
                "_action",
                request,
                authorizationInfo
            );
            assertThat(output.size(), is(logEntriesCount++));
            assertThat(output.get(logEntriesCount - 2), not(containsString("indices=")));
            auditTrail.authenticationSuccess(
                "_req_id",
                randomBoolean() ? createAuthentication() : createApiKeyAuthenticationAndMaybeWithRunAs(createAuthentication()),
                "_action",
                request
            );
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
        String logLine = singleLogLine(logger);
        if (checkFields == null) {
            // only check msg existence
            return;
        }
        assertMsg(logLine, checkFields, checkArrayFields);
    }

    private static String singleLogLine(Logger logger) {
        final List<String> output = CapturingLogger.output(logger.getName(), Level.INFO);
        assertThat("Exactly one logEntry expected. Found: " + output.size(), output.size(), is(1));
        return output.get(0);
    }

    private void assertMsg(String logLine, Map<String, String> checkFields, Map<String, String[]> checkArrayFields) {
        assertMsg(logLine, checkFields, checkArrayFields, Collections.emptyMap());
    }

    private void assertMsg(
        String logLine,
        Map<String, String> checkFields,
        Map<String, String[]> checkArrayFields,
        Map<String, String> checkLiteralFields
    ) {
        // check each string-valued field
        for (final Map.Entry<String, String> checkField : checkFields.entrySet()) {
            if (null == checkField.getValue()) {
                // null checkField means that the field does not exist
                assertThat(
                    "Field: " + checkField.getKey() + " should be missing.",
                    logLine.contains(Pattern.quote("\"" + checkField.getKey() + "\":")),
                    is(false)
                );
            } else {
                final String quotedValue = "\"" + checkField.getValue().replaceAll("\"", "\\\\\"") + "\"";
                final Pattern logEntryFieldPattern = Pattern.compile(Pattern.quote("\"" + checkField.getKey() + "\":" + quotedValue));
                assertThat(
                    "Field " + checkField.getKey() + " value mismatch. Expected " + quotedValue,
                    logEntryFieldPattern.matcher(logLine).find(),
                    is(true)
                );
                // remove checked field
                logLine = logEntryFieldPattern.matcher(logLine).replaceFirst("");
            }
        }
        // check each array-valued field
        for (final Map.Entry<String, String[]> checkArrayField : checkArrayFields.entrySet()) {
            if (null == checkArrayField.getValue()) {
                // null checkField means that the field does not exist
                assertThat(
                    "Field: " + checkArrayField.getKey() + " should be missing.",
                    logLine.contains(Pattern.quote("\"" + checkArrayField.getKey() + "\":")),
                    is(false)
                );
            } else {
                final String quotedValue = "["
                    + Arrays.asList(checkArrayField.getValue())
                        .stream()
                        .filter(s -> s != null)
                        .map(s -> "\"" + s.replaceAll("\"", "\\\\\"") + "\"")
                        .reduce((x, y) -> x + "," + y)
                        .orElse("")
                    + "]";
                final Pattern logEntryFieldPattern = Pattern.compile(Pattern.quote("\"" + checkArrayField.getKey() + "\":" + quotedValue));
                assertThat(
                    "Field " + checkArrayField.getKey() + " value mismatch. Expected " + quotedValue + ".\nLog line: " + logLine,
                    logEntryFieldPattern.matcher(logLine).find(),
                    is(true)
                );
                // remove checked field
                logLine = logEntryFieldPattern.matcher(logLine).replaceFirst("");
            }
        }
        // check each string-valued literal field
        for (final Map.Entry<String, String> checkField : checkLiteralFields.entrySet()) {
            assertThat("Use checkFields to mark missing fields", checkField.getValue(), notNullValue());
            final Pattern logEntryFieldPattern = Pattern.compile(Pattern.quote("\"" + checkField.getKey() + "\":" + checkField.getValue()));
            assertThat(
                "Field " + checkField.getKey() + " value mismatch. Expected " + checkField.getValue(),
                logEntryFieldPattern.matcher(logLine).find(),
                is(true)
            );
            // remove checked field
            logLine = logEntryFieldPattern.matcher(logLine).replaceFirst("");
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

    protected Tuple<Channel, RestRequest> prepareRestContent(String uri, InetSocketAddress remoteAddress) {
        return prepareRestContent(uri, remoteAddress, Collections.emptyMap());
    }

    private Tuple<Channel, RestRequest> prepareRestContent(String uri, InetSocketAddress remoteAddress, Map<String, String> params) {
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
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(remoteAddress);
        return new Tuple<>(channel, builder.build());
    }

    /** creates address without any lookups. hostname can be null, for missing */
    protected static InetAddress forge(String hostname, String address) throws IOException {
        final byte bytes[] = InetAddress.getByName(address).getAddress();
        return InetAddress.getByAddress(hostname, bytes);
    }

    private Authentication createAuthentication() {
        return randomValueOtherThanMany(
            authc -> authc.getAuthenticationType() == AuthenticationType.INTERNAL,
            () -> AuthenticationTestHelper.builder().build()
        );
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
            public void clearCredentials() {}
        };
    }

    private ClusterSettings mockClusterSettings() {
        final List<Setting<?>> settingsList = new ArrayList<>();
        LoggingAuditTrail.registerSettings(settingsList);
        settingsList.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new ClusterSettings(settings, new HashSet<>(settingsList));
    }

    private Authentication createApiKeyAuthenticationAndMaybeWithRunAs(Authentication authentication) throws Exception {
        authentication = createApiKeyAuthentication(apiKeyService, authentication);
        if (randomBoolean()) {
            authentication = authentication.runAs(AuthenticationTestHelper.randomUser(), AuthenticationTestHelper.randomRealmRef(false));
        }
        return authentication;
    }

    static class MockRequest extends TransportRequest {

        MockRequest(ThreadContext threadContext) throws IOException {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    remoteAddress(buildNewFakeTransportAddress().address());
                } else {
                    remoteAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234));
                }
            }
            if (randomBoolean()) {
                Channel mockChannel = mock(Channel.class);
                when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234));
                RemoteHostHeader.process(mockChannel, threadContext);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    static class MockIndicesRequest extends org.elasticsearch.action.MockIndicesRequest {

        MockIndicesRequest(ThreadContext threadContext) throws IOException {
            super(
                IndicesOptions.strictExpandOpenAndForbidClosed(),
                randomArray(0, 4, String[]::new, () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 4))
            );
            if (randomBoolean()) {
                remoteAddress(buildNewFakeTransportAddress().address());
            }
            if (randomBoolean()) {
                Channel mockChannel = mock(Channel.class);
                when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234));
                RemoteHostHeader.process(mockChannel, threadContext);
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

    private static void restOrTransportOrigin(
        TransportRequest request,
        ThreadContext threadContext,
        MapBuilder<String, String> checkedFields
    ) {
        final InetSocketAddress restAddress = RemoteHostHeader.restRemoteAddress(threadContext);
        if (restAddress != null) {
            checkedFields.put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.REST_ORIGIN_FIELD_VALUE)
                .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(restAddress));
        } else {
            final InetSocketAddress address = request.remoteAddress();
            if (address != null) {
                checkedFields.put(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, LoggingAuditTrail.TRANSPORT_ORIGIN_FIELD_VALUE)
                    .put(LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME, NetworkAddress.format(address));
            }
        }
    }

    private static void authentication(Authentication authentication, MapBuilder<String, String> checkedFields) {
        assertThat("use crossClusterAccessAuthentication instead", authentication.isCrossClusterAccess(), is(false));
        putCheckedFieldsForAuthentication(authentication, checkedFields);
    }

    private static void crossClusterAccessAuthentication(
        Authentication authentication,
        Authentication remoteAuthentication,
        MapBuilder<String, String> checkedFields,
        MapBuilder<String, String> checkedLiteralFields
    ) {
        assertThat("authentication must be cross cluster access", authentication.isCrossClusterAccess(), is(true));
        assertThat("remote authentication cannot be run-as", remoteAuthentication.isRunAs(), is(false));
        putCheckedFieldsForAuthentication(authentication, checkedFields);
        final String expectedCrossClusterAccessLiteralField = switch (remoteAuthentication.getEffectiveSubject().getType()) {
            case USER -> {
                assertThat(remoteAuthentication.getAuthenticationType(), oneOf(AuthenticationType.INTERNAL, AuthenticationType.REALM));
                yield Strings.format(
                    """
                        {"authentication.type":"%s","user.name":"%s","user.realm":"%s"}""",
                    remoteAuthentication.getAuthenticationType(),
                    remoteAuthentication.getEffectiveSubject().getUser().principal(),
                    remoteAuthentication.getEffectiveSubject().getRealm().getName()
                );
            }
            case API_KEY -> Strings.format(
                """
                    {"apikey.id":"%s","apikey.name":"%s","authentication.type":"API_KEY","user.name":"%s","user.realm":"%s"}""",
                remoteAuthentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY),
                remoteAuthentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_NAME_KEY),
                remoteAuthentication.getEffectiveSubject().getUser().principal(),
                remoteAuthentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME)
            );
            case SERVICE_ACCOUNT -> Strings.format(
                "{\"authentication.token.name\":\"%s\",\"authentication.token.type\":\"%s\","
                    + "\"authentication.type\":\"TOKEN\",\"user.name\":\"%s\",\"user.realm\":\"%s\"}",
                remoteAuthentication.getAuthenticatingSubject().getMetadata().get(TOKEN_NAME_FIELD),
                ServiceAccountSettings.REALM_TYPE
                    + "_"
                    + remoteAuthentication.getAuthenticatingSubject().getMetadata().get(TOKEN_SOURCE_FIELD),
                remoteAuthentication.getEffectiveSubject().getUser().principal(),
                remoteAuthentication.getEffectiveSubject().getRealm().getName()
            );
            default -> throw new IllegalArgumentException("unsupported type " + remoteAuthentication.getEffectiveSubject().getType());
        };
        checkedLiteralFields.put(LoggingAuditTrail.CROSS_CLUSTER_ACCESS_FIELD_NAME, expectedCrossClusterAccessLiteralField);
    }

    private static void putCheckedFieldsForAuthentication(Authentication authentication, MapBuilder<String, String> checkedFields) {
        checkedFields.put(LoggingAuditTrail.PRINCIPAL_FIELD_NAME, authentication.getEffectiveSubject().getUser().principal());
        checkedFields.put(LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME, authentication.getAuthenticationType().toString());
        if (authentication.isApiKey() || authentication.isCrossClusterAccess()) {
            assert false == authentication.isRunAs();
            checkedFields.put(
                LoggingAuditTrail.API_KEY_ID_FIELD_NAME,
                (String) authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_ID_KEY)
            );
            String apiKeyName = (String) authentication.getAuthenticatingSubject().getMetadata().get(AuthenticationField.API_KEY_NAME_KEY);
            if (apiKeyName != null) {
                checkedFields.put(LoggingAuditTrail.API_KEY_NAME_FIELD_NAME, apiKeyName);
            }
            String creatorRealmName = (String) authentication.getAuthenticatingSubject()
                .getMetadata()
                .get(AuthenticationField.API_KEY_CREATOR_REALM_NAME);
            if (creatorRealmName != null) {
                checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, creatorRealmName);
            }
        } else {
            final RealmRef authenticatedBy = authentication.getAuthenticatingSubject().getRealm();
            if (authentication.isRunAs()) {
                final RealmRef lookedUpBy = authentication.getEffectiveSubject().getRealm();
                if (lookedUpBy != null) {
                    checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, lookedUpBy.getName());
                    if (lookedUpBy.getDomain() != null) {
                        checkedFields.put(LoggingAuditTrail.PRINCIPAL_DOMAIN_FIELD_NAME, lookedUpBy.getDomain().name());
                    }
                }
                checkedFields.put(
                    LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME,
                    authentication.getAuthenticatingSubject().getUser().principal()
                ).put(LoggingAuditTrail.PRINCIPAL_RUN_BY_REALM_FIELD_NAME, authenticatedBy.getName());
                if (authenticatedBy.getDomain() != null) {
                    checkedFields.put(LoggingAuditTrail.PRINCIPAL_RUN_BY_DOMAIN_FIELD_NAME, authenticatedBy.getDomain().name());
                }
            } else {
                checkedFields.put(LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME, authenticatedBy.getName());
                if (authenticatedBy.getDomain() != null) {
                    checkedFields.put(LoggingAuditTrail.PRINCIPAL_DOMAIN_FIELD_NAME, authenticatedBy.getDomain().name());
                }
            }
        }
        if (authentication.isServiceAccount()) {
            checkedFields.put(
                LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME,
                (String) authentication.getAuthenticatingSubject().getMetadata().get(TOKEN_NAME_FIELD)
            )
                .put(
                    LoggingAuditTrail.SERVICE_TOKEN_TYPE_FIELD_NAME,
                    ServiceAccountSettings.REALM_TYPE
                        + "_"
                        + authentication.getAuthenticatingSubject().getMetadata().get(TOKEN_SOURCE_FIELD)
                );
        }
    }

    private static void opaqueId(ThreadContext threadContext, MapBuilder<String, String> checkedFields) {
        setFieldFromThreadContext(threadContext, checkedFields, Task.X_OPAQUE_ID_HTTP_HEADER, LoggingAuditTrail.OPAQUE_ID_FIELD_NAME);
    }

    private static void traceId(ThreadContext threadContext, MapBuilder<String, String> checkedFields) {
        setFieldFromThreadContext(threadContext, checkedFields, Task.TRACE_ID, LoggingAuditTrail.TRACE_ID_FIELD_NAME);
    }

    private static void forwardedFor(ThreadContext threadContext, MapBuilder<String, String> checkedFields) {
        setFieldFromThreadContext(
            threadContext,
            checkedFields,
            AuditTrail.X_FORWARDED_FOR_HEADER,
            LoggingAuditTrail.X_FORWARDED_FOR_FIELD_NAME
        );
    }

    private static void setFieldFromThreadContext(
        ThreadContext threadContext,
        MapBuilder<String, String> checkedFields,
        String threadContextFieldName,
        String logFieldName
    ) {
        final String value = threadContext.getHeader(threadContextFieldName);
        if (value != null) {
            checkedFields.put(logFieldName, value);
        }
    }

    private static void indicesRequest(
        TransportRequest request,
        MapBuilder<String, String> checkedFields,
        MapBuilder<String, String[]> checkedArrayFields
    ) {
        if (request instanceof IndicesRequest) {
            checkedFields.put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, MockIndicesRequest.class.getSimpleName());
            checkedArrayFields.put(LoggingAuditTrail.INDICES_FIELD_NAME, ((IndicesRequest) request).indices());
        } else {
            checkedFields.put(LoggingAuditTrail.REQUEST_NAME_FIELD_NAME, MockRequest.class.getSimpleName());
        }
    }

    private record ApiKeyMetadataWithSerialization(Map<String, Object> metadata, String serialization) {};

    private ApiKeyMetadataWithSerialization randomApiKeyMetadataWithSerialization() {
        final int metadataCase = randomInt(3);
        return switch (metadataCase) {
            case 0 -> new ApiKeyMetadataWithSerialization(null, null);
            case 1 -> new ApiKeyMetadataWithSerialization(Map.of(), "{}");
            case 2 -> {
                final Map<String, Object> metadata = new TreeMap<>();
                metadata.put("test", true);
                metadata.put("ans", 42);
                yield new ApiKeyMetadataWithSerialization(metadata, "{\"ans\":42,\"test\":true}");
            }
            case 3 -> {
                final Map<String, Object> metadata = new TreeMap<>();
                metadata.put("ans", List.of(42, true));
                metadata.put("other", Map.of("42", true));
                yield new ApiKeyMetadataWithSerialization(metadata, "{\"ans\":[42,true],\"other\":{\"42\":true}}");
            }
            default -> throw new IllegalStateException("Unexpected case number: " + metadataCase);
        };
    }

    private record CrossClusterApiKeyAccessWithSerialization(String access, String serialization) {};

    private CrossClusterApiKeyAccessWithSerialization randomCrossClusterApiKeyAccessWithSerialization() {
        return randomFrom(
            new CrossClusterApiKeyAccessWithSerialization(
                """
                    {
                      "search": [
                        {
                          "names": [
                            "logs*"
                          ]
                        }
                      ]
                    }""",
                "[{\"cluster\":[\"cross_cluster_search\"],"
                    + "\"indices\":[{\"names\":[\"logs*\"],"
                    + "\"privileges\":[\"read\",\"read_cross_cluster\",\"view_index_metadata\"]}],"
                    + "\"applications\":[],\"run_as\":[]}]"
            ),
            new CrossClusterApiKeyAccessWithSerialization(
                """
                    {
                      "replication": [
                        {
                          "names": [
                            "archive*"
                          ]
                        }
                      ]
                    }""",
                "[{\"cluster\":[\"cross_cluster_replication\"],"
                    + "\"indices\":[{\"names\":[\"archive*\"],"
                    + "\"privileges\":[\"cross_cluster_replication\",\"cross_cluster_replication_internal\"]}],"
                    + "\"applications\":[],\"run_as\":[]}]"
            ),
            new CrossClusterApiKeyAccessWithSerialization(
                """
                    {
                        "search": [
                          {
                            "names": [
                              "logs*"
                            ],
                            "query": {
                              "term": {
                                "tag": 42
                              }
                            },
                            "field_security": {
                              "grant": [
                                "*"
                              ],
                              "except": [
                                "private"
                              ]
                            }
                          }
                        ],
                        "replication": [
                          {
                            "names": [
                              "archive"
                            ],
                            "allow_restricted_indices": true
                          }
                        ]
                      }""",
                "[{\"cluster\":[\"cross_cluster_search\",\"cross_cluster_replication\"],"
                    + "\"indices\":[{\"names\":[\"logs*\"],\"privileges\":[\"read\",\"read_cross_cluster\",\"view_index_metadata\"],"
                    + "\"field_security\":{\"grant\":[\"*\"],\"except\":[\"private\"]},\"query\":\"{\\\"term\\\":{\\\"tag\\\":42}}\"},"
                    + "{\"names\":[\"archive\"],\"privileges\":[\"cross_cluster_replication\",\"cross_cluster_replication_internal\"],"
                    + "\"allow_restricted_indices\":true}],\"applications\":[],\"run_as\":[]}]"
            )
        );
    }
}
