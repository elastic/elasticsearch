/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.ssl.SSLService;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityTests extends ESTestCase {

    private Security security = null;

    public static class DummyExtension extends XPackExtension {
        private String realmType;
        DummyExtension(String realmType) {
            this.realmType = realmType;
        }
        @Override
        public String name() {
            return "dummy";
        }
        @Override
        public String description() {
            return "dummy";
        }
        @Override
        public Map<String, Realm.Factory> getRealms(ResourceWatcherService resourceWatcherService) {
            return Collections.singletonMap(realmType, config -> null);
        }
    }

    private Collection<Object> createComponents(Settings testSettings, XPackExtension... extensions) throws Exception {
        if (security != null) {
            throw new IllegalStateException("Security object already exists (" + security + ")");
        }
        Settings settings = Settings.builder().put(testSettings).put("path.home", createTempDir()).build();
        Environment env = new Environment(settings);
        security = new Security(settings, env, new XPackLicenseState(), new SSLService(settings, env));
        ThreadPool threadPool = mock(ThreadPool.class);
        ClusterService clusterService = mock(ClusterService.class);
        settings = Security.additionalSettings(settings, false);
        Set<Setting<?>> allowedSettings = new HashSet<>(Security.getSettings(false, null));
        allowedSettings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterSettings clusterSettings = new ClusterSettings(settings, allowedSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        InternalClient client = new InternalClient(Settings.EMPTY, threadPool, mock(Client.class));
        when(threadPool.relativeTimeInMillis()).thenReturn(1L);
        return security.createComponents(client, threadPool, clusterService, mock(ResourceWatcherService.class), Arrays.asList(extensions));
    }

    private <T> T findComponent(Class<T> type, Collection<Object> components) {
        for (Object obj : components) {
            if (type.isInstance(obj)) {
                return type.cast(obj);
            }
        }
        return null;
    }

    public void testCustomRealmExtension() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY, new DummyExtension("myrealm"));
        Realms realms = findComponent(Realms.class, components);
        assertNotNull(realms);
        assertNotNull(realms.realmFactory("myrealm"));
    }

    public void testCustomRealmExtensionConflict() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> createComponents(Settings.EMPTY, new DummyExtension(FileRealm.TYPE)));
        assertEquals("Realm type [" + FileRealm.TYPE + "] is already registered", e.getMessage());
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

    public void testIndexAuditTrail() throws Exception {
        Settings settings = Settings.builder()
            .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
            .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), "index").build();
        Collection<Object> components = createComponents(settings);
        AuditTrailService service = findComponent(AuditTrailService.class, components);
        assertNotNull(service);
        assertEquals(1, service.getAuditTrails().size());
        assertEquals(IndexAuditTrail.NAME, service.getAuditTrails().get(0).name());
    }

    public void testIndexAndLoggingAuditTrail() throws Exception {
        Settings settings = Settings.builder()
            .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
            .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), "index,logfile").build();
        Collection<Object> components = createComponents(settings);
        AuditTrailService service = findComponent(AuditTrailService.class, components);
        assertNotNull(service);
        assertEquals(2, service.getAuditTrails().size());
        assertEquals(IndexAuditTrail.NAME, service.getAuditTrails().get(0).name());
        assertEquals(LoggingAuditTrail.NAME, service.getAuditTrails().get(1).name());
    }

    public void testUnknownOutput() throws Exception {
        Settings settings = Settings.builder()
            .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
            .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), "foo").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createComponents(settings));
        assertEquals("Unknown audit trail output [foo]", e.getMessage());
    }

    public void testTransportSettingDefaults() throws Exception {
        Settings defaultSettings = Security.additionalSettings(Settings.EMPTY, false);
        assertEquals(Security.NAME4, NetworkModule.TRANSPORT_TYPE_SETTING.get(defaultSettings));
        assertEquals(Security.NAME4, NetworkModule.HTTP_TYPE_SETTING.get(defaultSettings));
    }

    public void testTransportSettingNetty4Both() {
        Settings both4 = Security.additionalSettings(Settings.builder()
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4)
            .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4)
            .build(), false);
        assertFalse(NetworkModule.TRANSPORT_TYPE_SETTING.exists(both4));
        assertFalse(NetworkModule.HTTP_TYPE_SETTING.exists(both4));
    }

    public void testTransportSettingValidation() {
        final String badType = randomFrom("netty4", "other", "security1");
        Settings settingsTransport = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, badType).build();
        IllegalArgumentException badTransport = expectThrows(IllegalArgumentException.class,
                () -> Security.additionalSettings(settingsTransport, false));
        assertThat(badTransport.getMessage(), containsString(Security.NAME4));
        assertThat(badTransport.getMessage(), containsString(NetworkModule.TRANSPORT_TYPE_KEY));

        Settings settingsHttp = Settings.builder().put(NetworkModule.HTTP_TYPE_KEY, badType).build();
        IllegalArgumentException badHttp = expectThrows(IllegalArgumentException.class,
                () -> Security.additionalSettings(settingsHttp, false));
        assertThat(badHttp.getMessage(), containsString(Security.NAME4));
        assertThat(badHttp.getMessage(), containsString(NetworkModule.HTTP_TYPE_KEY));
    }

    public void testSettingFilter() throws Exception {
        createComponents(Settings.EMPTY);
        final List<String> filter = security.getSettingsFilter(null);
        assertThat(filter, hasItem(Security.setting("authc.realms.*.bind_dn")));
        assertThat(filter, hasItem(Security.setting("authc.realms.*.bind_password")));
        assertThat(filter, hasItem(Security.setting("authc.realms.*." + SessionFactory.HOSTNAME_VERIFICATION_SETTING)));
        assertThat(filter, hasItem(Security.setting("authc.realms.*.ssl.truststore.password")));
        assertThat(filter, hasItem(Security.setting("authc.realms.*.ssl.truststore.path")));
        assertThat(filter, hasItem(Security.setting("authc.realms.*.ssl.truststore.algorithm")));
    }

    public void testTLSJoinValidatorOnDisabledSecurity() throws Exception {
        Settings disabledSettings = Settings.builder().put("xpack.security.enabled", false).build();
        createComponents(disabledSettings);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNull(joinValidator);
    }

    public void testTLSJoinValidator() throws Exception {
        createComponents(Settings.EMPTY);
        BiConsumer<DiscoveryNode, ClusterState> joinValidator = security.getJoinValidator();
        assertNotNull(joinValidator);
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        joinValidator.accept(node, ClusterState.builder(ClusterName.DEFAULT).build());
        assertTrue(joinValidator instanceof Security.ValidateTLSOnJoin);
        int numIters = randomIntBetween(1,10);
        for (int i = 0; i < numIters; i++) {
            boolean tlsOn = randomBoolean();
            Security.ValidateTLSOnJoin validator = new Security.ValidateTLSOnJoin(tlsOn);
            MetaData.Builder builder = MetaData.builder();
            License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
            TestUtils.putLicense(builder, license);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(builder.build()).build();
            EnumSet<License.OperationMode> productionModes = EnumSet.of(License.OperationMode.GOLD, License.OperationMode.PLATINUM,
                    License.OperationMode.STANDARD);
            if (productionModes.contains(license.operationMode()) && tlsOn == false) {
                IllegalStateException ise = expectThrows(IllegalStateException.class, () -> validator.accept(node, state));
                assertEquals("TLS setup is required for license type [" + license.operationMode().name() + "]", ise.getMessage());
            } else {
                validator.accept(node, state);
            }
            validator.accept(node, ClusterState.builder(ClusterName.DEFAULT).metaData(MetaData.builder().build()).build());
        }
    }
}
