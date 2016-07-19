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
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.file.FileRealm;

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
        Security security = new Security(settings, env);
        ThreadPool threadPool = mock(ThreadPool.class);
        return security.createComponents(null, threadPool, null, null, Arrays.asList(extensions));
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
        Settings settings = Settings.builder().put(Security.AUDIT_ENABLED_SETTING.getKey(), true).build();
        Collection<Object> components = createComponents(settings);
        AuditTrailService service = findComponent(AuditTrailService.class, components);
        assertNotNull(service);
        assertEquals(1, service.getAuditTrails().size());
        assertEquals(LoggingAuditTrail.NAME, service.getAuditTrails().get(0).name());
    }

    public void testDisabledByDefault() throws Exception {
        Collection<Object> components = createComponents(Settings.EMPTY);
        assertNull(findComponent(AuditTrailService.class, components));
    }

    public void testIndexAuditTrail() throws Exception {
        Settings settings = Settings.builder()
            .put(Security.AUDIT_ENABLED_SETTING.getKey(), true)
            .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), "index").build();
        Collection<Object> components = createComponents(settings);
        AuditTrailService service = findComponent(AuditTrailService.class, components);
        assertNotNull(service);
        assertEquals(1, service.getAuditTrails().size());
        assertEquals(IndexAuditTrail.NAME, service.getAuditTrails().get(0).name());
    }

    public void testIndexAndLoggingAuditTrail() throws Exception {
        Settings settings = Settings.builder()
            .put(Security.AUDIT_ENABLED_SETTING.getKey(), true)
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
            .put(Security.AUDIT_ENABLED_SETTING.getKey(), true)
            .put(Security.AUDIT_OUTPUTS_SETTING.getKey(), "foo").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createComponents(settings));
        assertEquals("Unknown audit trail output [foo]", e.getMessage());
    }
}
