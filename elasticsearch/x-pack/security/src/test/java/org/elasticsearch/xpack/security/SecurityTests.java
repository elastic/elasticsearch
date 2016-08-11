/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.file.FileRealm;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class SecurityTests extends ESTestCase {

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
        public Map<String, Realm.Factory> getRealms() {
            return Collections.singletonMap(realmType, config -> null);
        }
    }

    private Collection<Object> createComponents(Settings testSettings, XPackExtension... extensions) throws IOException {
        Settings settings = Settings.builder().put(testSettings)
            .put("path.home", createTempDir()).build();
        Environment env = new Environment(settings);
        Security security = new Security(settings, env, new XPackLicenseState());
        ThreadPool threadPool = mock(ThreadPool.class);
        ClusterService clusterService = mock(ClusterService.class);
        return security.createComponents(null, threadPool, clusterService, null, Arrays.asList(extensions));
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

    public void testTransportTypeSetting() throws Exception {
        Settings defaultSettings = Security.additionalSettings(Settings.EMPTY);
        assertEquals(Security.NAME4, NetworkModule.TRANSPORT_TYPE_SETTING.get(defaultSettings));
        assertEquals(Security.NAME4, NetworkModule.HTTP_TYPE_SETTING.get(defaultSettings));

        // set transport back to security3
        Settings transport3 = Security.additionalSettings(Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME3).build());
        assertFalse(NetworkModule.TRANSPORT_TYPE_SETTING.exists(transport3));
        assertEquals(Security.NAME4, NetworkModule.HTTP_TYPE_SETTING.get(transport3));

        // set http back to security3
        Settings http3 = Security.additionalSettings(Settings.builder().put(NetworkModule.HTTP_TYPE_KEY, Security.NAME3).build());
        assertEquals(Security.NAME4, NetworkModule.TRANSPORT_TYPE_SETTING.get(http3));
        assertFalse(NetworkModule.HTTP_TYPE_SETTING.exists(http3));

        // set both to security3
        Settings both3 = Security.additionalSettings(Settings.builder()
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME3)
                .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME3)
                .build());
        assertFalse(NetworkModule.TRANSPORT_TYPE_SETTING.exists(both3));
        assertFalse(NetworkModule.HTTP_TYPE_SETTING.exists(both3));

        // set both to 4
        Settings both4 = Security.additionalSettings(Settings.builder()
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4)
                .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4)
                .build());
        assertFalse(NetworkModule.TRANSPORT_TYPE_SETTING.exists(both4));
        assertFalse(NetworkModule.HTTP_TYPE_SETTING.exists(both4));

        final String badType = randomFrom("netty3", "netty4", "other", "security1");
        IllegalArgumentException badTransport = expectThrows(IllegalArgumentException.class,
                () -> Security.additionalSettings(Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, badType).build()));
        assertThat(badTransport.getMessage(), containsString(Security.NAME3));
        assertThat(badTransport.getMessage(), containsString(Security.NAME4));
        assertThat(badTransport.getMessage(), containsString(NetworkModule.TRANSPORT_TYPE_KEY));

        IllegalArgumentException badHttp = expectThrows(IllegalArgumentException.class,
                () -> Security.additionalSettings(Settings.builder().put(NetworkModule.HTTP_TYPE_KEY, badType).build()));
        assertThat(badHttp.getMessage(), containsString(Security.NAME3));
        assertThat(badHttp.getMessage(), containsString(Security.NAME4));
        assertThat(badHttp.getMessage(), containsString(NetworkModule.HTTP_TYPE_KEY));
    }
}
