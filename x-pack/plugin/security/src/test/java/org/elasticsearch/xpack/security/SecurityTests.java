/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.shard.IndexingStatsSettings;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.jwt.JwtRealm;
import org.elasticsearch.xpack.security.authc.service.CachingServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.FileServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.operator.DefaultOperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.operator.OperatorPrivilegesViolation;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;
import org.hamcrest.Matchers;
import org.junit.After;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.NOOP_OPERATOR_PRIVILEGES_SERVICE;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityTests extends ESTestCase {
    private Security security = null;
    private ThreadContext threadContext = null;
    private SecurityContext securityContext = null;
    private TestUtils.UpdatableLicenseState licenseState;

    public static class DummyExtension implements SecurityExtension {
        private final String realmType;
        private final ServiceAccountTokenStore serviceAccountTokenStore;
        private final String extensionName;

        DummyExtension(String realmType) {
            this(realmType, "DummyExtension", null);
        }

        DummyExtension(String realmType, String extensionName, @Nullable ServiceAccountTokenStore serviceAccountTokenStore) {
            this.realmType = realmType;
            this.extensionName = extensionName;
            this.serviceAccountTokenStore = serviceAccountTokenStore;
        }

        @Override
        public String extensionName() {
            return extensionName;
        }

        @Override
        public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
            return Collections.singletonMap(realmType, config -> null);
        }

        @Override
        public ServiceAccountTokenStore getServiceAccountTokenStore(SecurityComponents components) {
            return serviceAccountTokenStore;
        }
    }

    public static class DummyOperatorOnlyRegistry implements OperatorOnlyRegistry {
        @Override
        public OperatorPrivilegesViolation check(String action, TransportRequest request) {
            throw new RuntimeException("boom");
        }

        @Override
        public void checkRest(RestHandler restHandler, RestRequest restRequest) {
            throw new RuntimeException("boom");
        }
    }

    private void constructNewSecurityObject(Settings settings, SecurityExtension... extensions) {
        Environment env = TestEnvironment.newEnvironment(settings);
        licenseState = new TestUtils.UpdatableLicenseState(settings);
        SSLService sslService = new SSLService(env);
        if (security == null) {
            security = new Security(settings, Arrays.asList(extensions)) {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return licenseState;
                }

                @Override
                protected SSLService getSslService() {
                    return sslService;
                }
            };
        }
    }

    private Collection<Object> createComponentsUtil(Settings settings) throws Exception {
        Environment env = TestEnvironment.newEnvironment(settings);
        ThreadPool threadPool = mock(ThreadPool.class);
        ClusterService clusterService = mock(ClusterService.class);
        settings = Security.additionalSettings(settings, true);
        Set<Setting<?>> allowedSettings = new HashSet<>(Security.getSettings(null));
        allowedSettings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterSettings clusterSettings = new ClusterSettings(settings, allowedSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(threadPool.relativeTimeInMillis()).thenReturn(1L);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        return security.createComponents(
            client,
            threadPool,
            clusterService,
            new FeatureService(List.of(new SecurityFeatures())),
            mock(ResourceWatcherService.class),
            mock(ScriptService.class),
            xContentRegistry(),
            env,
            TestIndexNameExpressionResolver.newInstance(threadContext),
            TelemetryProvider.NOOP,
            mock(PersistentTasksService.class),
            TestProjectResolvers.alwaysThrow()
        );
    }

    private Collection<Object> createComponents(Settings testSettings, SecurityExtension... extensions) throws Exception {
        if (security != null) {
            throw new IllegalStateException("Security object already exists (" + security + ")");
        }
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put(testSettings)
            .put("path.home", createTempDir())
            .build();
        constructNewSecurityObject(settings, extensions);
        return createComponentsUtil(settings);
    }

    private static <T> T findComponent(Class<T> type, Collection<Object> components) {
        for (Object obj : components) {
            if (type.isInstance(obj)) {
                return type.cast(obj);
            }
        }
        return null;
    }

    @After
    public void cleanup() {
        threadContext = null;
    }

    public void testCustomRealmExtension() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY, new DummyExtension("myrealm"));
        Realms realms = findComponent(Realms.class, components);
        assertNotNull(realms);
        assertNotNull(realms.realmFactory("myrealm"));
    }

    public void testCustomRealmExtensionConflict() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createComponents(Settings.EMPTY, new DummyExtension(FileRealmSettings.TYPE))
        );
        assertEquals("Realm type [" + FileRealmSettings.TYPE + "] is already registered", e.getMessage());
    }

    public void testServiceAccountTokenStoreExtensionSuccess() throws Exception {
        Collection<Object> components = createComponents(
            Settings.EMPTY,
            new DummyExtension(
                "test_realm",
                "DummyExtension",
                (token, listener) -> listener.onResponse(
                    ServiceAccountTokenStore.StoreAuthenticationResult.successful(TokenInfo.TokenSource.FILE)
                )
            )
        );
        ServiceAccountService serviceAccountService = findComponent(ServiceAccountService.class, components);
        assertNotNull(serviceAccountService);
        FileServiceAccountTokenStore fileServiceAccountTokenStore = findComponent(FileServiceAccountTokenStore.class, components);
        assertNull(fileServiceAccountTokenStore);
        IndexServiceAccountTokenStore indexServiceAccountTokenStore = findComponent(IndexServiceAccountTokenStore.class, components);
        assertNull(indexServiceAccountTokenStore);
        var account = randomFrom(ServiceAccountService.getServiceAccounts().values());
        assertThrows(IllegalStateException.class, () -> serviceAccountService.createIndexToken(null, null, null));
        var future = new PlainActionFuture<Authentication>();
        serviceAccountService.authenticateToken(ServiceAccountToken.newToken(account.id(), "test"), "test", future);
        assertTrue(future.get().isServiceAccount());
    }

    public void testSeveralServiceAccountTokenStoreExtensionFail() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> createComponents(
                Settings.EMPTY,
                new DummyExtension(
                    "test_realm_1",
                    "DummyExtension1",
                    (token, listener) -> listener.onResponse(
                        ServiceAccountTokenStore.StoreAuthenticationResult.successful(TokenInfo.TokenSource.FILE)
                    )
                ),
                new DummyExtension(
                    "test_realm_2",
                    "DummyExtension2",
                    (token, listener) -> listener.onResponse(
                        ServiceAccountTokenStore.StoreAuthenticationResult.successful(TokenInfo.TokenSource.FILE)
                    )
                )
            )
        );
        assertThat(exception.getMessage(), containsString("More than one extension provided a ServiceAccountTokenStore override: "));
    }

    public void testNoServiceAccountTokenStoreExtension() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY);
        ServiceAccountService serviceAccountService = findComponent(ServiceAccountService.class, components);
        assertNotNull(serviceAccountService);
        FileServiceAccountTokenStore fileServiceAccountTokenStore = findComponent(FileServiceAccountTokenStore.class, components);
        assertNotNull(fileServiceAccountTokenStore);
        IndexServiceAccountTokenStore indexServiceAccountTokenStore = findComponent(IndexServiceAccountTokenStore.class, components);
        assertNotNull(indexServiceAccountTokenStore);
    }

    public void testAuditEnabled() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.AUDIT_ENABLED.getKey(), true).build();
        Collection<Object> components = createComponents(settings);
        AuditTrailService service = findComponent(AuditTrailService.class, components);
        assertNotNull(service);
        assertThat(service.getAuditTrail(), notNullValue());
        assertEquals(LoggingAuditTrail.NAME, service.getAuditTrail().name());
    }

    public void testDisabledByDefault() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY);
        AuditTrailService auditTrailService = findComponent(AuditTrailService.class, components);
        assertThat(auditTrailService.getAuditTrail(), nullValue());
    }

    public void testHttpSettingDefaults() throws Exception {
        final Settings defaultSettings = Security.additionalSettings(Settings.EMPTY, true);
        assertThat(SecurityField.NAME4, equalTo(NetworkModule.TRANSPORT_TYPE_SETTING.get(defaultSettings)));
        assertThat(SecurityField.NAME4, equalTo(NetworkModule.HTTP_TYPE_SETTING.get(defaultSettings)));
    }

    public void testTransportSettingNetty4Both() {
        Settings both4 = Security.additionalSettings(
            Settings.builder()
                .put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4)
                .put(NetworkModule.HTTP_TYPE_KEY, SecurityField.NAME4)
                .build(),
            true
        );
        assertFalse(NetworkModule.TRANSPORT_TYPE_SETTING.exists(both4));
        assertFalse(NetworkModule.HTTP_TYPE_SETTING.exists(both4));
    }

    public void testTransportSettingValidation() {
        final String badType = randomFrom("netty4", "other", "security1");
        Settings settingsTransport = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, badType).build();
        IllegalArgumentException badTransport = expectThrows(
            IllegalArgumentException.class,
            () -> Security.additionalSettings(settingsTransport, true)
        );
        assertThat(badTransport.getMessage(), containsString(SecurityField.NAME4));
        assertThat(badTransport.getMessage(), containsString(NetworkModule.TRANSPORT_TYPE_KEY));

        Settings settingsHttp = Settings.builder().put(NetworkModule.HTTP_TYPE_KEY, badType).build();
        IllegalArgumentException badHttp = expectThrows(
            IllegalArgumentException.class,
            () -> Security.additionalSettings(settingsHttp, true)
        );
        assertThat(badHttp.getMessage(), containsString(SecurityField.NAME4));
        assertThat(badHttp.getMessage(), containsString(NetworkModule.HTTP_TYPE_KEY));
    }

    public void testNoRealmsWhenSecurityDisabled() throws Exception {
        Settings settings = Settings.builder()
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("path.home", createTempDir())
            .build();
        Collection<Object> components = createComponents(settings);
        for (Object component : components) {
            assertThat(component, not(instanceOf(Realms.class)));
            assertThat(component, not(instanceOf(NativeUsersStore.class)));
            assertThat(component, not(instanceOf(ReservedRealm.class)));
        }
    }

    public void testSettingFilter() throws Exception {
        createComponents(Settings.EMPTY);
        final List<String> filter = security.getSettingsFilter();
        assertThat(filter, hasItem("transport.profiles.*.xpack.security.*"));
    }

    public void testOnIndexModuleIsNoOpWithSecurityDisabled() throws Exception {
        Settings settings = Settings.builder()
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("path.home", createTempDir())
            .build();
        createComponents(settings);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        AnalysisRegistry emptyAnalysisRegistry = new AnalysisRegistry(
            TestEnvironment.newEnvironment(settings),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        IndexModule indexModule = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            Collections.emptyMap(),
            () -> true,
            TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
            Collections.emptyMap(),
            mock(SlowLogFieldProvider.class),
            MapperMetrics.NOOP,
            List.of(),
            new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings()),
            new SearchStatsSettings(ClusterSettings.createBuiltInClusterSettings()),
            MergeMetrics.NOOP
        );
        security.onIndexModule(indexModule);
        // indexReaderWrapper is a SetOnce so if Security#onIndexModule had already set an ReaderWrapper we would get an exception here
        indexModule.setReaderWrapper(null);
    }

    public void testFilteredSettings() throws Exception {
        createComponents(Settings.EMPTY);
        final List<Setting<?>> realmSettings = security.getSettings()
            .stream()
            .filter(s -> s.getKey().startsWith("xpack.security.authc.realms"))
            .collect(Collectors.toList());

        Arrays.asList(
            "bind_dn",
            "bind_password",
            "hostname_verification",
            "truststore.password",
            "truststore.path",
            "truststore.algorithm",
            "trust_restrictions.path",
            "trust_restrictions.x509_fields",
            "keystore.key_password"
        ).forEach(suffix -> {

            final List<Setting<?>> matching = realmSettings.stream()
                .filter(s -> s.getKey().endsWith("." + suffix))
                .collect(Collectors.toList());
            assertThat("For suffix " + suffix, matching, Matchers.not(empty()));
            matching.forEach(
                setting -> assertThat("For setting " + setting, setting.getProperties(), Matchers.hasItem(Setting.Property.Filtered))
            );
        });
    }

    public void testJoinValidatorOnDisabledSecurity() throws Exception {
        Settings disabledSettings = Settings.builder().put("xpack.security.enabled", false).build();
        createComponents(disabledSettings);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNull(joinValidator);
    }

    public void testJoinValidatorForFIPSOnAllowedLicense() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.builder("foo")
            .version(VersionUtils.randomVersion(random()), IndexVersions.ZERO, IndexVersionUtils.randomVersion())
            .build();
        Metadata.Builder builder = Metadata.builder();
        License license = TestUtils.generateSignedLicense(
            randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.PLATINUM, License.OperationMode.TRIAL).toString(),
            TimeValue.timeValueHours(24)
        );
        TestUtils.putLicense(builder, license);
        LicenseService licenseService;
        if (randomBoolean()) {
            licenseService = mock(ClusterStateLicenseService.class);
            when(((ClusterStateLicenseService) licenseService).getLicense(any())).thenReturn(license);
        } else {
            licenseService = mock(LicenseService.class);
            when(licenseService.getLicense()).thenReturn(license);
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder.build()).build();
        new Security.ValidateLicenseForFIPS(false, licenseService).accept(node, state);
        // no exception thrown
        new Security.ValidateLicenseForFIPS(true, licenseService).accept(node, state);
        // no exception thrown
    }

    public void testJoinValidatorForFIPSOnForbiddenLicense() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.builder("foo")
            .version(VersionUtils.randomVersion(random()), IndexVersions.ZERO, IndexVersionUtils.randomVersion())
            .build();
        Metadata.Builder builder = Metadata.builder();
        final String forbiddenLicenseType = randomFrom(
            List.of(License.OperationMode.values())
                .stream()
                .filter(l -> XPackLicenseState.isFipsAllowedForOperationMode(l) == false)
                .collect(Collectors.toList())
        ).toString();
        License license = TestUtils.generateSignedLicense(forbiddenLicenseType, TimeValue.timeValueHours(24));
        TestUtils.putLicense(builder, license);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder.build()).build();
        ClusterStateLicenseService licenseService = mock(ClusterStateLicenseService.class);
        when(licenseService.getLicense(any())).thenReturn(license);
        new Security.ValidateLicenseForFIPS(false, licenseService).accept(node, state);
        // no exception thrown
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new Security.ValidateLicenseForFIPS(true, licenseService).accept(node, state)
        );
        assertThat(e.getMessage(), containsString("FIPS mode cannot be used"));
    }

    public void testGetFieldFilterSecurityEnabled() throws Exception {
        createComponents(Settings.EMPTY);
        Function<String, FieldPredicate> fieldFilter = security.getFieldFilter();
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
        Map<String, IndicesAccessControl.IndexAccessControl> permissionsMap = new HashMap<>();

        FieldPermissions permissions = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "field_granted" }, Strings.EMPTY_ARRAY)
        );
        IndicesAccessControl.IndexAccessControl indexGrantedAccessControl = new IndicesAccessControl.IndexAccessControl(
            permissions,
            DocumentPermissions.allowAll()
        );
        permissionsMap.put("index_granted", indexGrantedAccessControl);
        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(
            randomFrom(FieldPermissions.DEFAULT, null),
            randomFrom(DocumentPermissions.allowAll(), null)
        );
        permissionsMap.put("index_granted_all_permissions", indexAccessControl);
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, permissionsMap);
        securityContext.putIndicesAccessControl(indicesAccessControl);

        assertThat(fieldFilter.apply("index_granted"), trueWith("field_granted"));
        assertThat(fieldFilter.apply("index_granted"), falseWith(randomAlphaOfLengthBetween(3, 10)));
        assertEquals(FieldPredicate.ACCEPT_ALL, fieldFilter.apply("index_granted_all_permissions"));
        assertThat(fieldFilter.apply("index_granted_all_permissions"), trueWith(randomAlphaOfLengthBetween(3, 10)));
        assertEquals(FieldPredicate.ACCEPT_ALL, fieldFilter.apply("index_other"));
    }

    public void testGetFieldFilterSecurityDisabled() throws Exception {
        createComponents(Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build());
        assertSame(MapperPlugin.NOOP_FIELD_FILTER, security.getFieldFilter());
    }

    public void testGetFieldFilterSecurityEnabledLicenseNoFLS() throws Exception {
        createComponents(Settings.EMPTY);
        Function<String, FieldPredicate> fieldFilter = security.getFieldFilter();
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
        licenseState.update(
            new XPackLicenseStatus(
                randomFrom(License.OperationMode.BASIC, License.OperationMode.STANDARD, License.OperationMode.GOLD),
                true,
                null
            )
        );
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
        assertSame(FieldPredicate.ACCEPT_ALL, fieldFilter.apply(randomAlphaOfLengthBetween(3, 6)));
    }

    public void testValidateRealmsWhenSettingsAreInvalid() {
        final Settings settings = Settings.builder()
            .put(RealmSettings.PREFIX + "my_pki.type", "pki")
            .put(RealmSettings.PREFIX + "ldap1.type", "ldap")
            .build();
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateRealmSettings(settings));
        assertThat(iae.getMessage(), containsString("Incorrect realm settings"));
        assertThat(iae.getMessage(), containsString("breaking changes doc"));
        assertThat(iae.getMessage(), containsString(RealmSettings.PREFIX + "my_pki.type"));
        assertThat(iae.getMessage(), containsString(RealmSettings.PREFIX + "ldap1.type"));
    }

    public void testValidateRealmsWhenSettingsAreCorrect() {
        final Settings settings = Settings.builder()
            .put(RealmSettings.PREFIX + "pki.my_pki.order", 0)
            .put(RealmSettings.PREFIX + "ldap.ldap1.order", 1)
            .build();
        Security.validateRealmSettings(settings);
        // no-exception
    }

    public void testValidateForFipsKeystoreWithImplicitJksType() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put("xpack.security.transport.ssl.keystore.path", "path/to/keystore")
            .put(
                XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2") == false)
                        .collect(Collectors.toList())
                )
            )
            .build();
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
        assertThat(iae.getMessage(), containsString("JKS Keystores cannot be used in a FIPS 140 compliant JVM"));
    }

    public void testValidateForFipsKeystoreWithExplicitJksType() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put("xpack.security.transport.ssl.keystore.path", "path/to/keystore")
            .put("xpack.security.transport.ssl.keystore.type", "JKS")
            .put(
                XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2"))
                        .collect(Collectors.toList())
                )
            )
            .build();
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
        assertThat(iae.getMessage(), containsString("JKS Keystores cannot be used in a FIPS 140 compliant JVM"));
    }

    public void testValidateForFipsInvalidPasswordHashingAlgorithm() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(
                XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2") == false)
                        .collect(Collectors.toList())
                )
            )
            .build();
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
        assertThat(iae.getMessage(), containsString("Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM."));
    }

    public void testValidateForFipsRequiredProvider() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .putList(XPackSettings.FIPS_REQUIRED_PROVIDERS.getKey(), List.of("BCFIPS"))
            .build();
        if (inFipsJvm()) {
            Security.validateForFips(settings);
            // no exceptions since gradle has wired in the bouncy castle FIPS provider
        } else {
            final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
            assertThat(iae.getMessage(), containsString("Could not find required FIPS security provider: [bcfips]"));
        }

        final Settings settings2 = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .putList(XPackSettings.FIPS_REQUIRED_PROVIDERS.getKey(), List.of("junk0", "BCFIPS", "junk1", "junk2"))
            .build();
        if (inFipsJvm()) {
            final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings2));
            assertThat(iae.getMessage(), containsString("Could not find required FIPS security provider: [junk0, junk1, junk2]"));
        } else {
            final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings2));
            assertThat(iae.getMessage(), containsString("Could not find required FIPS security provider: [junk0, bcfips, junk1, junk2]"));
        }
    }

    public void testValidateForFipsMultipleValidationErrors() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put("xpack.security.transport.ssl.keystore.path", "path/to/keystore")
            .put("xpack.security.transport.ssl.keystore.type", "JKS")
            .put(
                XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2") == false)
                        .collect(Collectors.toList())
                )
            )
            .build();
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
        assertThat(iae.getMessage(), containsString("JKS Keystores cannot be used in a FIPS 140 compliant JVM"));
        assertThat(iae.getMessage(), containsString("Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM."));
    }

    public void testValidateForFipsNoErrorsOrLogs() throws IllegalAccessException {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put("xpack.security.transport.ssl.keystore.path", "path/to/keystore")
            .put("xpack.security.transport.ssl.keystore.type", "BCFKS")
            .put(
                XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2"))
                        .collect(Collectors.toList())
                )
            )
            .put(
                XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2"))
                        .collect(Collectors.toList())
                )
            )
            .put(
                ApiKeyService.STORED_HASH_ALGO_SETTING.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredPasswordHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2"))
                        .collect(Collectors.toList())
                )
            )
            .put(
                ApiKeyService.CACHE_HASH_ALGO_SETTING.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoCacheHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2") || alg.equals("ssha256"))
                        .collect(Collectors.toList())
                )
            )
            .build();
        assertThatLogger(() -> Security.validateForFips(settings), Security.class);
    }

    public void testValidateForFipsNonFipsCompliantCacheHashAlgoWarningLog() throws IllegalAccessException {
        String key = randomCacheHashSetting();
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(key, randomNonFipsCompliantCacheHash())
            .build();
        assertThatLogger(() -> Security.validateForFips(settings), Security.class, logEventForNonCompliantCacheHash(key));
    }

    public void testValidateForFipsNonFipsCompliantStoredHashAlgoWarningLog() {
        String key = XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey();
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(key, randomNonFipsCompliantStoredPasswordHash())
            .build();
        assertThatLogger(() -> Security.validateForFips(settings), Security.class, logEventForNonCompliantStoredPasswordHash(key));
    }

    public void testValidateForFipsNonFipsCompliantApiKeyStoredHashAlgoWarningLog() {
        var nonCompliant = randomFrom(
            Hasher.getAvailableAlgoStoredPasswordHash()
                .stream()
                .filter(alg -> alg.startsWith("pbkdf2") == false && alg.startsWith("ssha256") == false)
                .collect(Collectors.toList())
        );
        String key = ApiKeyService.STORED_HASH_ALGO_SETTING.getKey();
        final Settings settings = Settings.builder().put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true).put(key, nonCompliant).build();
        assertThatLogger(() -> Security.validateForFips(settings), Security.class, logEventForNonCompliantStoredApiKeyHash(key));
    }

    public void testValidateForFipsFipsCompliantApiKeyStoredHashAlgoWarningLog() {
        var compliant = randomFrom(
            Hasher.getAvailableAlgoStoredPasswordHash()
                .stream()
                .filter(alg -> alg.startsWith("pbkdf2") || alg.startsWith("ssha256"))
                .collect(Collectors.toList())
        );
        String key = ApiKeyService.STORED_HASH_ALGO_SETTING.getKey();
        final Settings settings = Settings.builder().put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true).put(key, compliant).build();
        assertThatLogger(() -> Security.validateForFips(settings), Security.class);
    }

    public void testValidateForMultipleNonFipsCompliantCacheHashAlgoWarningLogs() throws IllegalAccessException {
        String firstKey = randomCacheHashSetting();
        String secondKey = randomValueOtherThan(firstKey, this::randomCacheHashSetting);
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(firstKey, randomNonFipsCompliantCacheHash())
            .put(secondKey, randomNonFipsCompliantCacheHash())
            .build();
        assertThatLogger(
            () -> Security.validateForFips(settings),
            Security.class,
            logEventForNonCompliantCacheHash(firstKey),
            logEventForNonCompliantCacheHash(secondKey)
        );
    }

    public void testValidateForFipsValidationErrorAndWarningLogs() throws IllegalAccessException {
        String firstKey = randomCacheHashSetting();
        String secondKey = randomValueOtherThan(firstKey, this::randomCacheHashSetting);
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(firstKey, randomNonFipsCompliantCacheHash())
            .put(secondKey, randomNonFipsCompliantCacheHash())
            .put("xpack.security.transport.ssl.keystore.path", "path/to/keystore")
            .build();
        assertThatLogger(() -> {
            final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
            assertThat(iae.getMessage(), containsString("JKS Keystores cannot be used in a FIPS 140 compliant JVM"));
        }, Security.class, logEventForNonCompliantCacheHash(firstKey), logEventForNonCompliantCacheHash(secondKey));
    }

    public void testValidateForFipsNoErrorsOrLogsForDefaultSettings() throws IllegalAccessException {
        final Settings settings = Settings.builder().put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true).build();
        assertThatLogger(() -> Security.validateForFips(settings), Security.class);
    }

    public void testLicenseUpdateFailureHandlerUpdate() throws Exception {
        final Path kerbKeyTab = createTempFile("es", "keytab");
        Files.write(kerbKeyTab, new byte[0]);
        Settings settings = Settings.builder()
            .put("xpack.security.authc.api_key.enabled", "true")
            .put("xpack.security.authc.realms.kerberos.kb.enabled", true)
            .put("xpack.security.authc.realms.kerberos.kb.order", 2)
            .put("xpack.security.authc.realms.kerberos.kb.keytab.path", kerbKeyTab)
            .build();
        Collection<Object> components = createComponents(settings);
        AuthenticationService service = findComponent(AuthenticationService.class, components);
        assertNotNull(service);
        RestRequest request = new FakeRestRequest();
        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate(
            request.getHttpRequest(),
            ActionListener.wrap(result -> { assertTrue(completed.compareAndSet(false, true)); }, e -> {
                // On trial license, kerberos is allowed and the WWW-Authenticate response header should reflect that
                verifyHasAuthenticationHeaderValue(
                    e,
                    "Basic realm=\"" + XPackField.SECURITY + "\", charset=\"UTF-8\"",
                    "Negotiate",
                    "ApiKey"
                );
            })
        );
        threadContext.stashContext();
        licenseState.update(new XPackLicenseStatus(randomFrom(License.OperationMode.GOLD, License.OperationMode.BASIC), true, null));
        service.authenticate(
            request.getHttpRequest(),
            ActionListener.wrap(result -> { assertTrue(completed.compareAndSet(false, true)); }, e -> {
                // On basic or gold license, kerberos is not allowed and the WWW-Authenticate response header should also reflect that
                verifyHasAuthenticationHeaderValue(e, "Basic realm=\"" + XPackField.SECURITY + "\", charset=\"UTF-8\"", "ApiKey");
            })
        );
        if (completed.get()) {
            fail("authentication succeeded but it shouldn't");
        }
    }

    public void testSecurityPluginInstallsRestHandlerInterceptorEvenIfSecurityIsDisabled() throws IllegalAccessException {
        Settings settings = Settings.builder().put("xpack.security.enabled", false).put("path.home", createTempDir()).build();
        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());

        try {
            UsageService usageService = new UsageService();
            Security security = new Security(settings);
            assertTrue(security.getRestHandlerInterceptor(threadPool.getThreadContext()) != null);

        } finally {
            threadPool.shutdown();
        }

    }

    public void testSecurityRestHandlerInterceptorCanBeInstalled() throws IllegalAccessException {
        final Logger amLogger = LogManager.getLogger(ActionModule.class);
        Loggers.setLevel(amLogger, Level.DEBUG);

        Settings settings = Settings.builder().put("xpack.security.enabled", false).put("path.home", createTempDir()).build();
        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());

        try (var mockLog = MockLog.capture(ActionModule.class)) {
            UsageService usageService = new UsageService();
            Security security = new Security(settings);

            // Verify Security rest interceptor is about to be installed
            // We will throw later if another interceptor is already installed
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Security rest interceptor",
                    ActionModule.class.getName(),
                    Level.DEBUG,
                    "Using custom REST interceptor from plugin org.elasticsearch.xpack.security.Security"
                )
            );

            ActionModule actionModule = new ActionModule(
                settingsModule.getSettings(),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                null,
                settingsModule.getIndexScopedSettings(),
                settingsModule.getClusterSettings(),
                settingsModule.getSettingsFilter(),
                threadPool,
                List.of(security),
                null,
                null,
                usageService,
                null,
                TelemetryProvider.NOOP,
                mock(ClusterService.class),
                null,
                List.of(),
                List.of(),
                RestExtension.allowAll(),
                new IncrementalBulkService(null, null),
                TestProjectResolvers.alwaysThrow()
            );
            actionModule.initRestHandlers(null, null);

            mockLog.assertAllExpectationsMatched();
        } finally {
            threadPool.shutdown();
        }
    }

    public void testSecurityStatusMessageInLog() throws Exception {
        final Logger mockLogger = LogManager.getLogger(Security.class);
        boolean securityEnabled = true;
        Loggers.setLevel(mockLogger, Level.INFO);

        Settings.Builder settings = Settings.builder().put("path.home", createTempDir());
        if (randomBoolean()) {
            // randomize explicit vs implicit configuration
            securityEnabled = randomBoolean();
            settings.put("xpack.security.enabled", securityEnabled);
        }

        try (var mockLog = MockLog.capture(Security.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "message",
                    Security.class.getName(),
                    Level.INFO,
                    "Security is " + (securityEnabled ? "enabled" : "disabled")
                )
            );
            createComponents(settings.build());
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testSecurityMustBeEnableToConnectRemoteClusterWithCredentials() {
        // Security on, no remote cluster with credentials
        final Settings.Builder builder1 = Settings.builder();
        if (randomBoolean()) {
            builder1.put("xpack.security.enabled", "true");
        }
        try {
            new Security(builder1.build());
        } catch (Exception e) {
            fail("Security should have been successfully initialized, but got " + e.getMessage());
        }

        // Security off, no remote cluster with credentials
        final Settings.Builder builder2 = Settings.builder().put("xpack.security.enabled", "false");
        try {
            new Security(builder2.build());
        } catch (Exception e) {
            fail("Security should have been successfully initialized, but got " + e.getMessage());
        }

        // Security on, remote cluster with credentials
        final Settings.Builder builder3 = Settings.builder();
        final MockSecureSettings secureSettings3 = new MockSecureSettings();
        final String clusterCredentials3 = randomAlphaOfLength(20);
        secureSettings3.setString("cluster.remote.my1.credentials", clusterCredentials3);
        builder3.setSecureSettings(secureSettings3);
        if (randomBoolean()) {
            builder3.put("xpack.security.enabled", "true");
        }
        final Settings settings3 = builder3.build();
        try {
            new Security(settings3);
        } catch (Exception e) {
            fail("Security should have been successfully initialized, but got " + e.getMessage());
        }

        // Security off, remote cluster with credentials
        final Settings.Builder builder4 = Settings.builder();
        final MockSecureSettings secureSettings4 = new MockSecureSettings();
        secureSettings4.setString("cluster.remote.my1.credentials", randomAlphaOfLength(20));
        secureSettings4.setString("cluster.remote.my2.credentials", randomAlphaOfLength(20));
        builder4.setSecureSettings(secureSettings4);
        final Settings settings4 = builder4.put("xpack.security.enabled", "false").build();
        final IllegalArgumentException e4 = expectThrows(IllegalArgumentException.class, () -> new Security(settings4));
        assertThat(
            e4.getMessage(),
            containsString(
                "Found [2] remote clusters with credentials [cluster.remote.my1.credentials,cluster.remote.my2.credentials]. "
                    + "Security [xpack.security.enabled] must be enabled to connect to them. "
                    + "Please either enable security or remove these settings from the keystore."
            )
        );

        // Security off, remote cluster with credentials on reload call
        final MockSecureSettings secureSettings5 = new MockSecureSettings();
        secureSettings5.setString("cluster.remote.my1.credentials", randomAlphaOfLength(20));
        secureSettings5.setString("cluster.remote.my2.credentials", randomAlphaOfLength(20));
        final Settings.Builder builder5 = Settings.builder().setSecureSettings(secureSettings5);
        // Use builder with security disabled to construct valid Security instance
        final var security = new Security(builder2.build());
        final IllegalArgumentException e5 = expectThrows(IllegalArgumentException.class, () -> security.reload(builder5.build()));
        assertThat(
            e5.getMessage(),
            containsString(
                "Found [2] remote clusters with credentials [cluster.remote.my1.credentials,cluster.remote.my2.credentials]. "
                    + "Security [xpack.security.enabled] must be enabled to connect to them. "
                    + "Please either enable security or remove these settings from the keystore."
            )
        );
    }

    public void testLoadExtensions() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("path.home", createTempDir())
            .put(OPERATOR_PRIVILEGES_ENABLED.getKey(), true)
            .build();
        constructNewSecurityObject(settings);
        security.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                List<Object> extensions = new ArrayList<>();
                if (extensionPointType == OperatorOnlyRegistry.class) {
                    extensions.add(new DummyOperatorOnlyRegistry());
                }
                return (List<T>) extensions;
            }
        });
        createComponentsUtil(settings);
        OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService = security.getOperatorPrivilegesService();
        assertThat(operatorPrivilegesService, instanceOf(OperatorPrivileges.DefaultOperatorPrivilegesService.class));
        OperatorOnlyRegistry registry = ((OperatorPrivileges.DefaultOperatorPrivilegesService) operatorPrivilegesService)
            .getOperatorOnlyRegistry();
        assertThat(registry, instanceOf(DummyOperatorOnlyRegistry.class));
    }

    public void testReload() throws Exception {
        final Settings settings = Settings.builder().put("xpack.security.enabled", true).put("path.home", createTempDir()).build();

        final ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final Client mockedClient = mock(Client.class);
        when(mockedClient.threadPool()).thenReturn(threadPool);

        final JwtRealm mockedJwtRealm = mock(JwtRealm.class);
        final List<ReloadableSecurityComponent> reloadableComponents = List.of(mockedJwtRealm);

        doAnswer((inv) -> {
            @SuppressWarnings("unchecked")
            ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) inv.getArguments()[2];
            listener.onResponse(ActionResponse.Empty.INSTANCE);
            return null;
        }).when(mockedClient).execute(eq(ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION), any(), any());

        security = new Security(settings, Collections.emptyList()) {
            @Override
            protected Client getClient() {
                return mockedClient;
            }

            @Override
            protected List<ReloadableSecurityComponent> getReloadableSecurityComponents() {
                return reloadableComponents;
            }
        };

        final Settings inputSettings = Settings.EMPTY;
        security.reload(inputSettings);

        verify(mockedClient).execute(eq(ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION), any(), any());
        verify(mockedJwtRealm).reload(same(inputSettings));
    }

    public void testReloadWithFailures() {
        final Settings settings = Settings.builder().put("xpack.security.enabled", true).put("path.home", createTempDir()).build();

        final boolean failRemoteClusterCredentialsReload = randomBoolean();

        final ThreadPool threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final Client mockedClient = mock(Client.class);
        when(mockedClient.threadPool()).thenReturn(threadPool);

        final JwtRealm mockedJwtRealm = mock(JwtRealm.class);
        final List<ReloadableSecurityComponent> reloadableComponents = List.of(mockedJwtRealm);
        if (failRemoteClusterCredentialsReload) {
            doAnswer((inv) -> {
                @SuppressWarnings("unchecked")
                ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) inv.getArguments()[2];
                listener.onFailure(new RuntimeException("failed remote cluster credentials reload"));
                return null;
            }).when(mockedClient).execute(eq(ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION), any(), any());
        } else {
            doAnswer((inv) -> {
                @SuppressWarnings("unchecked")
                ActionListener<ActionResponse.Empty> listener = (ActionListener<ActionResponse.Empty>) inv.getArguments()[2];
                listener.onResponse(ActionResponse.Empty.INSTANCE);
                return null;
            }).when(mockedClient).execute(eq(ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION), any(), any());
        }

        final boolean failRealmsReload = (false == failRemoteClusterCredentialsReload) || randomBoolean();
        if (failRealmsReload) {
            doThrow(new RuntimeException("failed jwt realms reload")).when(mockedJwtRealm).reload(any());
        }
        security = new Security(settings, Collections.emptyList()) {
            @Override
            protected Client getClient() {
                return mockedClient;
            }

            @Override
            protected List<ReloadableSecurityComponent> getReloadableSecurityComponents() {
                return reloadableComponents;
            }
        };

        final Settings inputSettings = Settings.EMPTY;
        final var exception = expectThrows(ElasticsearchException.class, () -> security.reload(inputSettings));

        assertThat(exception.getMessage(), containsString("secure settings reload failed for one or more security component"));
        if (failRemoteClusterCredentialsReload) {
            assertThat(exception.getSuppressed()[0].getMessage(), containsString("failed remote cluster credentials reload"));
            if (failRealmsReload) {
                assertThat(exception.getSuppressed()[1].getMessage(), containsString("failed jwt realms reload"));
            }
        } else {
            assertThat(exception.getSuppressed()[0].getMessage(), containsString("failed jwt realms reload"));
        }
        // Verify both called despite failure
        verify(mockedClient).execute(eq(ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION), any(), any());
        verify(mockedJwtRealm).reload(same(inputSettings));
    }

    public void testLoadNoExtensions() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("path.home", createTempDir())
            .put(OPERATOR_PRIVILEGES_ENABLED.getKey(), true)
            .build();
        constructNewSecurityObject(settings);
        security.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return new ArrayList<>();
            }
        });
        createComponentsUtil(settings);
        OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService = security.getOperatorPrivilegesService();
        assertThat(operatorPrivilegesService, instanceOf(OperatorPrivileges.DefaultOperatorPrivilegesService.class));
        OperatorOnlyRegistry registry = ((OperatorPrivileges.DefaultOperatorPrivilegesService) operatorPrivilegesService)
            .getOperatorOnlyRegistry();
        assertThat(registry, instanceOf(DefaultOperatorOnlyRegistry.class));

    }

    public void testLoadExtensionsWhenOperatorPrivsAreDisabled() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder().put("xpack.security.enabled", true).put("path.home", createTempDir());

        if (randomBoolean()) {
            settingsBuilder.put(OPERATOR_PRIVILEGES_ENABLED.getKey(), false); // doesn't matter if explicit or implicitly disabled
        }

        Settings settings = settingsBuilder.build();
        constructNewSecurityObject(settings);
        security.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                List<Object> extensions = new ArrayList<>();
                if (extensionPointType == OperatorOnlyRegistry.class) {
                    if (randomBoolean()) {
                        extensions.add(new DummyOperatorOnlyRegistry()); // won't ever be used
                    }
                }
                return (List<T>) extensions;
            }
        });
        createComponentsUtil(settings);
        OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService = security.getOperatorPrivilegesService();
        assertThat(operatorPrivilegesService, is(NOOP_OPERATOR_PRIVILEGES_SERVICE));
    }

    private void verifyHasAuthenticationHeaderValue(Exception e, String... expectedValues) {
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate"), notNullValue());
        assertThat(((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate"), hasSize(expectedValues.length));
        assertThat(((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate"), containsInAnyOrder(expectedValues));
    }

    private String randomCacheHashSetting() {
        Authentication.RealmRef ref = AuthenticationTestHelper.randomRealmRef(randomBoolean());
        return randomFrom(
            getFullSettingKey(
                new RealmConfig.RealmIdentifier(ref.getType(), ref.getName()),
                CachingUsernamePasswordRealmSettings.CACHE_HASH_ALGO_SETTING
            ),
            CachingServiceAccountTokenStore.CACHE_HASH_ALGO_SETTING.getKey(),
            ApiKeyService.CACHE_HASH_ALGO_SETTING.getKey()
        );
    }

    private String randomNonFipsCompliantCacheHash() {
        return randomFrom(
            Hasher.getAvailableAlgoCacheHash()
                .stream()
                .filter(alg -> (alg.startsWith("pbkdf2") || alg.equals("ssha256")) == false)
                .collect(Collectors.toList())
        );
    }

    private String randomNonFipsCompliantStoredPasswordHash() {
        return randomFrom(
            Hasher.getAvailableAlgoStoredPasswordHash()
                .stream()
                .filter(alg -> alg.startsWith("pbkdf2") == false)
                .collect(Collectors.toList())
        );
    }

    private MockLog.SeenEventExpectation logEventForNonCompliantCacheHash(String settingKey) {
        return new MockLog.SeenEventExpectation(
            "cache hash not fips compliant",
            Security.class.getName(),
            Level.WARN,
            "[*] is not recommended for in-memory credential hashing in a FIPS 140 JVM. "
                + "The recommended hasher for ["
                + settingKey
                + "] is SSHA256."
        );
    }

    private MockLog.SeenEventExpectation logEventForNonCompliantStoredApiKeyHash(String settingKey) {
        return new MockLog.SeenEventExpectation(
            "cache hash not fips compliant",
            Security.class.getName(),
            Level.WARN,
            "[*] is not recommended for stored API key hashing in a FIPS 140 JVM. "
                + "The recommended hasher for ["
                + settingKey
                + "] is SSHA256."
        );
    }

    private MockLog.SeenEventExpectation logEventForNonCompliantStoredPasswordHash(String settingKey) {
        return new MockLog.SeenEventExpectation(
            "stored hash not fips compliant",
            Security.class.getName(),
            Level.WARN,
            "Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM. "
                + "Please set the appropriate value for ["
                + settingKey
                + "] setting."
        );
    }
}
