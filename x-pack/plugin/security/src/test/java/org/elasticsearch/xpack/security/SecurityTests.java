/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.Realms;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.INTERNAL_MAIN_INDEX_FORMAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityTests extends ESTestCase {

    private Security security = null;
    private ThreadContext threadContext = null;
    private TestUtils.UpdatableLicenseState licenseState;

    public static class DummyExtension implements SecurityExtension {
        private String realmType;

        DummyExtension(String realmType) {
            this.realmType = realmType;
        }

        @Override
        public Map<String, Realm.Factory> getRealms(ResourceWatcherService resourceWatcherService) {
            return Collections.singletonMap(realmType, config -> null);
        }
    }

    private Collection<Object> createComponents(Settings testSettings, SecurityExtension... extensions) throws Exception {
        if (security != null) {
            throw new IllegalStateException("Security object already exists (" + security + ")");
        }
        Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put(testSettings)
            .put("path.home", createTempDir()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        licenseState = new TestUtils.UpdatableLicenseState(settings);
        SSLService sslService = new SSLService(settings, env);
        security = new Security(settings, null, Arrays.asList(extensions)) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }

            @Override
            protected SSLService getSslService() {
                return sslService;
            }
        };
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
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        return security.createComponents(client, threadPool, clusterService, mock(ResourceWatcherService.class), mock(ScriptService.class));
    }

    private static <T> T findComponent(Class<T> type, Collection<Object> components) {
        for (Object obj : components) {
            if (type.isInstance(obj)) {
                return type.cast(obj);
            }
        }
        return null;
    }

    @Before
    public void cleanup() throws IOException {
        if (threadContext != null) {
            threadContext.stashContext();
            threadContext.close();
            threadContext = null;
        }
    }

    public void testCustomRealmExtension() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY, new DummyExtension("myrealm"));
        Realms realms = findComponent(Realms.class, components);
        assertNotNull(realms);
        assertNotNull(realms.realmFactory("myrealm"));
    }

    public void testCustomRealmExtensionConflict() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> createComponents(Settings.EMPTY, new DummyExtension(FileRealmSettings.TYPE)));
        assertEquals("Realm type [" + FileRealmSettings.TYPE + "] is already registered", e.getMessage());
    }


    public void testAuditEnabled() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.AUDIT_ENABLED.getKey(), true).build();
        Collection<Object> components = createComponents(settings);
        AuditTrailService service = findComponent(AuditTrailService.class, components);
        assertNotNull(service);
        assertEquals(1, service.getAuditTrails().size());
        assertEquals(LoggingAuditTrail.NAME, service.getAuditTrails().get(0).name());
    }

    public void testDisabledByDefault() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY);
        AuditTrailService auditTrailService = findComponent(AuditTrailService.class, components);
        assertEquals(0, auditTrailService.getAuditTrails().size());
    }

    public void testHttpSettingDefaults() throws Exception {
        final Settings defaultSettings = Security.additionalSettings(Settings.EMPTY, true);
        assertThat(SecurityField.NAME4, equalTo(NetworkModule.TRANSPORT_TYPE_SETTING.get(defaultSettings)));
        assertThat(SecurityField.NAME4, equalTo(NetworkModule.HTTP_TYPE_SETTING.get(defaultSettings)));
    }

    public void testTransportSettingNetty4Both() {
        Settings both4 = Security.additionalSettings(Settings.builder()
            .put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4)
            .put(NetworkModule.HTTP_TYPE_KEY, SecurityField.NAME4)
            .build(), true);
        assertFalse(NetworkModule.TRANSPORT_TYPE_SETTING.exists(both4));
        assertFalse(NetworkModule.HTTP_TYPE_SETTING.exists(both4));
    }

    public void testTransportSettingValidation() {
        final String badType = randomFrom("netty4", "other", "security1");
        Settings settingsTransport = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, badType).build();
        IllegalArgumentException badTransport = expectThrows(IllegalArgumentException.class,
            () -> Security.additionalSettings(settingsTransport, true));
        assertThat(badTransport.getMessage(), containsString(SecurityField.NAME4));
        assertThat(badTransport.getMessage(), containsString(NetworkModule.TRANSPORT_TYPE_KEY));

        Settings settingsHttp = Settings.builder().put(NetworkModule.HTTP_TYPE_KEY, badType).build();
        IllegalArgumentException badHttp = expectThrows(IllegalArgumentException.class,
            () -> Security.additionalSettings(settingsHttp, true));
        assertThat(badHttp.getMessage(), containsString(SecurityField.NAME4));
        assertThat(badHttp.getMessage(), containsString(NetworkModule.HTTP_TYPE_KEY));
    }

    public void testSettingFilter() throws Exception {
        createComponents(Settings.EMPTY);
        final List<String> filter = security.getSettingsFilter();
        assertThat(filter, hasItem("transport.profiles.*.xpack.security.*"));
    }

    public void testFilteredSettings() throws Exception {
        createComponents(Settings.EMPTY);
        final List<Setting<?>> realmSettings = security.getSettings().stream()
            .filter(s -> s.getKey().startsWith("xpack.security.authc.realms"))
            .collect(Collectors.toList());

        Arrays.asList(
            "bind_dn", "bind_password",
            "hostname_verification",
            "truststore.password", "truststore.path", "truststore.algorithm",
            "keystore.key_password").forEach(suffix -> {

            final List<Setting<?>> matching = realmSettings.stream()
                .filter(s -> s.getKey().endsWith("." + suffix))
                .collect(Collectors.toList());
            assertThat("For suffix " + suffix, matching, Matchers.not(empty()));
            matching.forEach(setting -> assertThat("For setting " + setting,
                setting.getProperties(), Matchers.hasItem(Setting.Property.Filtered)));
        });
    }

    public void testJoinValidatorOnDisabledSecurity() throws Exception {
        Settings disabledSettings = Settings.builder().put("xpack.security.enabled", false).build();
        createComponents(disabledSettings);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNull(joinValidator);
    }

    public void testJoinValidatorForFIPSLicense() throws Exception {
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            VersionUtils.randomVersionBetween(random(), null, Version.CURRENT));
        MetaData.Builder builder = MetaData.builder();
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        TestUtils.putLicense(builder, license);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(builder.build()).build();
        new Security.ValidateLicenseForFIPS(false).accept(node, state);

        final boolean isLicenseValidForFips =
            FIPS140LicenseBootstrapCheck.ALLOWED_LICENSE_OPERATION_MODES.contains(license.operationMode());
        if (isLicenseValidForFips) {
            new Security.ValidateLicenseForFIPS(true).accept(node, state);
        } else {
            IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> new Security.ValidateLicenseForFIPS(true).accept(node, state));
            assertThat(e.getMessage(), containsString("FIPS mode cannot be used"));
        }
    }

    public void testIndexJoinValidator_FullyCurrentCluster() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        int indexFormat = randomBoolean() ? INTERNAL_MAIN_INDEX_FORMAT : INTERNAL_MAIN_INDEX_FORMAT - 1;
        IndexMetaData indexMetaData = IndexMetaData.builder(SECURITY_MAIN_ALIAS)
            .settings(settings(VersionUtils.randomIndexCompatibleVersion(random())).put(INDEX_FORMAT_SETTING.getKey(), indexFormat))
            .numberOfShards(1).numberOfReplicas(0)
            .build();
        DiscoveryNode existingOtherNode = new DiscoveryNode("bar", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(existingOtherNode).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metaData(MetaData.builder().put(indexMetaData, true).build()).build();
        joinValidator.accept(node, clusterState);
    }

    public void testIndexUpgradeValidatorWithUpToDateIndex() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        IndexMetaData indexMetaData = IndexMetaData.builder(SECURITY_MAIN_ALIAS)
            .settings(settings(version).put(INDEX_FORMAT_SETTING.getKey(), INTERNAL_MAIN_INDEX_FORMAT))
            .numberOfShards(1).numberOfReplicas(0)
            .build();
        DiscoveryNode existingOtherNode = new DiscoveryNode("bar", buildNewFakeTransportAddress(), version);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(existingOtherNode).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metaData(MetaData.builder().put(indexMetaData, true).build()).build();
        joinValidator.accept(node, clusterState);
    }

    public void testIndexUpgradeValidatorWithMissingIndex() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode existingOtherNode = new DiscoveryNode("bar", buildNewFakeTransportAddress(),
            VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(existingOtherNode).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes).build();
        joinValidator.accept(node, clusterState);
    }

    public void testGetFieldFilterSecurityEnabled() throws Exception {
        createComponents(Settings.EMPTY);
        Function<String, Predicate<String>> fieldFilter = security.getFieldFilter();
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
        Map<String, IndicesAccessControl.IndexAccessControl> permissionsMap = new HashMap<>();

        FieldPermissions permissions = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "field_granted" }, Strings.EMPTY_ARRAY));
        IndicesAccessControl.IndexAccessControl indexGrantedAccessControl = new IndicesAccessControl.IndexAccessControl(true, permissions,
                DocumentPermissions.allowAll());
        permissionsMap.put("index_granted", indexGrantedAccessControl);
        IndicesAccessControl.IndexAccessControl indexAccessControl = new IndicesAccessControl.IndexAccessControl(false,
                FieldPermissions.DEFAULT, DocumentPermissions.allowAll());
        permissionsMap.put("index_not_granted", indexAccessControl);
        IndicesAccessControl.IndexAccessControl nullFieldPermissions =
                new IndicesAccessControl.IndexAccessControl(true, null, DocumentPermissions.allowAll());
        permissionsMap.put("index_null", nullFieldPermissions);
        IndicesAccessControl index = new IndicesAccessControl(true, permissionsMap);
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, index);

        assertTrue(fieldFilter.apply("index_granted").test("field_granted"));
        assertFalse(fieldFilter.apply("index_granted").test(randomAlphaOfLengthBetween(3, 10)));
        assertTrue(fieldFilter.apply(randomAlphaOfLengthBetween(3, 6)).test("field_granted"));
        assertTrue(fieldFilter.apply(randomAlphaOfLengthBetween(3, 6)).test(randomAlphaOfLengthBetween(3, 10)));
        assertEquals(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply(randomAlphaOfLengthBetween(3, 10)));
        expectThrows(IllegalStateException.class, () -> fieldFilter.apply("index_not_granted"));
        assertTrue(fieldFilter.apply("index_null").test(randomAlphaOfLengthBetween(3, 6)));
        assertEquals(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply("index_null"));
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
            randomFrom(License.OperationMode.BASIC, License.OperationMode.STANDARD, License.OperationMode.GOLD), true, null);
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
}
