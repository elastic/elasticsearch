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
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
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
import org.elasticsearch.xpack.security.authc.service.CachingServiceAccountTokenStore;
import org.elasticsearch.xpack.security.operator.DefaultOperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorOnlyRegistry;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.operator.OperatorPrivilegesViolation;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.NOOP_OPERATOR_PRIVILEGES_SERVICE;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityTests extends ESTestCase {

    private Security security = null;
    private ThreadContext threadContext = null;
    private SecurityContext securityContext = null;
    private TestUtils.UpdatableLicenseState licenseState;

    public static class DummyExtension implements SecurityExtension {
        private String realmType;

        DummyExtension(String realmType) {
            this.realmType = realmType;
        }

        @Override
        public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
            return Collections.singletonMap(realmType, config -> null);
        }
    }

    public static class DummyOperatorOnlyRegistry implements OperatorOnlyRegistry {
        @Override
        public OperatorPrivilegesViolation check(String action, TransportRequest request) {
            throw new RuntimeException("boom");
        }

        @Override
        public OperatorPrivilegesViolation checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel) {
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
        NodeMetadata nodeMetadata = new NodeMetadata(randomAlphaOfLength(8), Version.CURRENT, Version.CURRENT);
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
            mock(ResourceWatcherService.class),
            mock(ScriptService.class),
            xContentRegistry(),
            env,
            nodeMetadata,
            TestIndexNameExpressionResolver.newInstance(threadContext)
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

    public void testCustomRealmExtensionConflict() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createComponents(Settings.EMPTY, new DummyExtension(FileRealmSettings.TYPE))
        );
        assertEquals("Realm type [" + FileRealmSettings.TYPE + "] is already registered", e.getMessage());
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
            Collections.emptyMap()
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
            .version(VersionUtils.randomVersionBetween(random(), null, Version.CURRENT))
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
            .version(VersionUtils.randomVersionBetween(random(), null, Version.CURRENT))
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

    public void testIndexJoinValidator_FullyCurrentCluster() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");
        int indexFormat = randomBoolean() ? INTERNAL_MAIN_INDEX_FORMAT : INTERNAL_MAIN_INDEX_FORMAT - 1;
        IndexMetadata indexMetadata = IndexMetadata.builder(SECURITY_MAIN_ALIAS)
            .settings(settings(VersionUtils.randomIndexCompatibleVersion(random())).put(INDEX_FORMAT_SETTING.getKey(), indexFormat))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DiscoveryNode existingOtherNode = DiscoveryNodeUtils.create("bar");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(existingOtherNode).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();
        joinValidator.accept(node, clusterState);
    }

    public void testIndexUpgradeValidatorWithUpToDateIndex() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");
        IndexMetadata indexMetadata = IndexMetadata.builder(SECURITY_MAIN_ALIAS)
            .settings(settings(version).put(INDEX_FORMAT_SETTING.getKey(), INTERNAL_MAIN_INDEX_FORMAT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DiscoveryNode existingOtherNode = DiscoveryNodeUtils.builder("bar").version(version).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(existingOtherNode).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();
        joinValidator.accept(node, clusterState);
    }

    public void testIndexUpgradeValidatorWithMissingIndex() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");
        DiscoveryNode existingOtherNode = DiscoveryNodeUtils.builder("bar")
            .version(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT))
            .build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(existingOtherNode).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();
        joinValidator.accept(node, clusterState);
    }

    public void testGetFieldFilterSecurityEnabled() throws Exception {
        createComponents(Settings.EMPTY);
        Function<String, Predicate<String>> fieldFilter = security.getFieldFilter();
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

        assertTrue(fieldFilter.apply("index_granted").test("field_granted"));
        assertFalse(fieldFilter.apply("index_granted").test(randomAlphaOfLengthBetween(3, 10)));
        assertEquals(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply("index_granted_all_permissions"));
        assertTrue(fieldFilter.apply("index_granted_all_permissions").test(randomAlphaOfLengthBetween(3, 10)));
        assertEquals(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply("index_other"));
    }

    public void testGetFieldFilterSecurityDisabled() throws Exception {
        createComponents(Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build());
        assertSame(MapperPlugin.NOOP_FIELD_FILTER, security.getFieldFilter());
    }

    public void testGetFieldFilterSecurityEnabledLicenseNoFLS() throws Exception {
        createComponents(Settings.EMPTY);
        Function<String, Predicate<String>> fieldFilter = security.getFieldFilter();
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
        licenseState.update(
            new XPackLicenseStatus(
                randomFrom(License.OperationMode.BASIC, License.OperationMode.STANDARD, License.OperationMode.GOLD),
                true,
                null
            )
        );
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
        assertSame(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply(randomAlphaOfLengthBetween(3, 6)));
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
                    Hasher.getAvailableAlgoStoredHash()
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
                    Hasher.getAvailableAlgoStoredHash().stream().filter(alg -> alg.startsWith("pbkdf2")).collect(Collectors.toList())
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
                    Hasher.getAvailableAlgoStoredHash()
                        .stream()
                        .filter(alg -> alg.startsWith("pbkdf2") == false)
                        .collect(Collectors.toList())
                )
            )
            .build();
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
        assertThat(iae.getMessage(), containsString("Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM."));
    }

    public void testValidateForFipsMultipleValidationErrors() {
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put("xpack.security.transport.ssl.keystore.path", "path/to/keystore")
            .put("xpack.security.transport.ssl.keystore.type", "JKS")
            .put(
                XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredHash()
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
                    Hasher.getAvailableAlgoStoredHash().stream().filter(alg -> alg.startsWith("pbkdf2")).collect(Collectors.toList())
                )
            )
            .put(
                XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredHash().stream().filter(alg -> alg.startsWith("pbkdf2")).collect(Collectors.toList())
                )
            )
            .put(
                ApiKeyService.PASSWORD_HASHING_ALGORITHM.getKey(),
                randomFrom(
                    Hasher.getAvailableAlgoStoredHash().stream().filter(alg -> alg.startsWith("pbkdf2")).collect(Collectors.toList())
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
        expectLogs(Security.class, Collections.emptyList(), () -> Security.validateForFips(settings));
    }

    public void testValidateForFipsNonFipsCompliantCacheHashAlgoWarningLog() throws IllegalAccessException {
        String key = randomCacheHashSetting();
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(key, randomNonFipsCompliantCacheHash())
            .build();
        expectLogs(Security.class, List.of(logEventForNonCompliantCacheHash(key)), () -> Security.validateForFips(settings));
    }

    public void testValidateForFipsNonFipsCompliantStoredHashAlgoWarningLog() throws IllegalAccessException {
        String key = randomFrom(ApiKeyService.PASSWORD_HASHING_ALGORITHM, XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM).getKey();
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(key, randomNonFipsCompliantStoredHash())
            .build();
        expectLogs(Security.class, List.of(logEventForNonCompliantStoredHash(key)), () -> Security.validateForFips(settings));
    }

    public void testValidateForMultipleNonFipsCompliantCacheHashAlgoWarningLogs() throws IllegalAccessException {
        String firstKey = randomCacheHashSetting();
        String secondKey = randomValueOtherThan(firstKey, this::randomCacheHashSetting);
        final Settings settings = Settings.builder()
            .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
            .put(firstKey, randomNonFipsCompliantCacheHash())
            .put(secondKey, randomNonFipsCompliantCacheHash())
            .build();
        expectLogs(
            Security.class,
            List.of(logEventForNonCompliantCacheHash(firstKey), logEventForNonCompliantCacheHash(secondKey)),
            () -> Security.validateForFips(settings)
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
        expectLogs(Security.class, List.of(logEventForNonCompliantCacheHash(firstKey), logEventForNonCompliantCacheHash(secondKey)), () -> {
            final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Security.validateForFips(settings));
            assertThat(iae.getMessage(), containsString("JKS Keystores cannot be used in a FIPS 140 compliant JVM"));
        });
    }

    public void testValidateForFipsNoErrorsOrLogsForDefaultSettings() throws IllegalAccessException {
        final Settings settings = Settings.builder().put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true).build();
        expectLogs(Security.class, Collections.emptyList(), () -> Security.validateForFips(settings));
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
                    "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"",
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
                verifyHasAuthenticationHeaderValue(e, "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"", "ApiKey");
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
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(amLogger, appender);
        appender.start();

        Settings settings = Settings.builder().put("xpack.security.enabled", false).put("path.home", createTempDir()).build();
        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());

        try {
            UsageService usageService = new UsageService();
            Security security = new Security(settings);

            // Verify Security rest interceptor is about to be installed
            // We will throw later if another interceptor is already installed
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Security rest interceptor",
                    ActionModule.class.getName(),
                    Level.DEBUG,
                    "Using REST interceptor from plugin org.elasticsearch.xpack.security.Security"
                )
            );

            ActionModule actionModule = new ActionModule(
                settingsModule.getSettings(),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                settingsModule.getIndexScopedSettings(),
                settingsModule.getClusterSettings(),
                settingsModule.getSettingsFilter(),
                threadPool,
                Arrays.asList(security),
                null,
                null,
                usageService,
                null,
                Tracer.NOOP,
                mock(ClusterService.class),
                List.of()
            );
            actionModule.initRestHandlers(null);

            appender.assertAllExpectationsMatched();
        } finally {
            threadPool.shutdown();
            appender.stop();
            Loggers.removeAppender(amLogger, appender);
        }
    }

    public void testSecurityStatusMessageInLog() throws Exception {
        final Logger mockLogger = LogManager.getLogger(Security.class);
        boolean securityEnabled = true;
        Loggers.setLevel(mockLogger, Level.INFO);
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(mockLogger, appender);
        appender.start();

        Settings.Builder settings = Settings.builder().put("path.home", createTempDir());
        if (randomBoolean()) {
            // randomize explicit vs implicit configuration
            securityEnabled = randomBoolean();
            settings.put("xpack.security.enabled", securityEnabled);
        }

        try {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "message",
                    Security.class.getName(),
                    Level.INFO,
                    "Security is " + (securityEnabled ? "enabled" : "disabled")
                )
            );
            createComponents(settings.build());
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.removeAppender(mockLogger, appender);
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
        assumeFalse("feature flag for serverless is expected to be false", DiscoveryNode.isServerless());
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

    public void testLoadExtensionsWhenOperatorPrivsAreDisabledAndServerless() throws Exception {
        assumeTrue("feature flag for serverless is expected to be true", DiscoveryNode.isServerless());
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
                        extensions.add(new DummyOperatorOnlyRegistry()); // will be used
                    }
                }
                return (List<T>) extensions;
            }
        });
        createComponentsUtil(settings);
        OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService = security.getOperatorPrivilegesService();
        OperatorOnlyRegistry registry = ((OperatorPrivileges.DefaultOperatorPrivilegesService) operatorPrivilegesService)
            .getOperatorOnlyRegistry();
        assertThat(registry, instanceOf(DummyOperatorOnlyRegistry.class));
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

    private String randomNonFipsCompliantStoredHash() {
        return randomFrom(
            Hasher.getAvailableAlgoStoredHash().stream().filter(alg -> alg.startsWith("pbkdf2") == false).collect(Collectors.toList())
        );
    }

    private MockLogAppender.SeenEventExpectation logEventForNonCompliantCacheHash(String settingKey) {
        return new MockLogAppender.SeenEventExpectation(
            "cache hash not fips compliant",
            Security.class.getName(),
            Level.WARN,
            "[*] is not recommended for in-memory credential hashing in a FIPS 140 JVM. "
                + "The recommended hasher for ["
                + settingKey
                + "] is SSHA256."
        );
    }

    private MockLogAppender.SeenEventExpectation logEventForNonCompliantStoredHash(String settingKey) {
        return new MockLogAppender.SeenEventExpectation(
            "stored hash not fips compliant",
            Security.class.getName(),
            Level.WARN,
            "Only PBKDF2 is allowed for stored credential hashing in a FIPS 140 JVM. "
                + "Please set the appropriate value for ["
                + settingKey
                + "] setting."
        );
    }

    private void expectLogs(Class<?> clazz, List<MockLogAppender.LoggingExpectation> expected, Runnable runnable)
        throws IllegalAccessException {
        final MockLogAppender mockAppender = new MockLogAppender();
        final Logger logger = LogManager.getLogger(clazz);
        mockAppender.start();
        try {
            Loggers.addAppender(logger, mockAppender);
            expected.forEach(mockAppender::addExpectation);
            runnable.run();
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, mockAppender);
            mockAppender.stop();
        }
    }
}
