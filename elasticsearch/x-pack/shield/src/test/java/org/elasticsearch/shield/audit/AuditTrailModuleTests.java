/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.shield.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.local.LocalTransport;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class AuditTrailModuleTests extends ESTestCase {
    public void testEnabled() throws Exception {
        Settings settings = Settings.builder()
                .put("client.type", "node")
                .put("shield.audit.enabled", false)
                .build();
        SettingsModule settingsModule = new SettingsModule(settings, new SettingsFilter(settings));
        settingsModule.registerSetting(Setting.boolSetting("shield.audit.enabled", true, false, Setting.Scope.CLUSTER));
        Injector injector = Guice.createInjector(settingsModule, new AuditTrailModule(settings));
        AuditTrail auditTrail = injector.getInstance(AuditTrail.class);
        assertThat(auditTrail, is(AuditTrail.NOOP));
    }

    public void testDisabledByDefault() throws Exception {
        Settings settings = Settings.builder()
                .put("client.type", "node").build();
        Injector injector = Guice.createInjector(new SettingsModule(settings, new SettingsFilter(settings)), new AuditTrailModule(settings));
        AuditTrail auditTrail = injector.getInstance(AuditTrail.class);
        assertThat(auditTrail, is(AuditTrail.NOOP));
    }

    public void testLogfile() throws Exception {
        Settings settings = Settings.builder()
                .put("shield.audit.enabled", true)
                .put("client.type", "node")
                .build();
        ThreadPool pool = new ThreadPool("testLogFile");
        try {
            SettingsModule settingsModule = new SettingsModule(settings, new SettingsFilter(settings));
            settingsModule.registerSetting(Setting.boolSetting("shield.audit.enabled", true, false, Setting.Scope.CLUSTER));
            Injector injector = Guice.createInjector(
                    settingsModule,
                    new NetworkModule(new NetworkService(settings), settings, false, null) {
                        @Override
                        protected void configure() {
                            bind(Transport.class).to(LocalTransport.class).asEagerSingleton();
                        }
                    },
                    new AuditTrailModule(settings),
                    new CircuitBreakerModule(settings),
                    new ThreadPoolModule(pool),
                    new Version.Module(Version.CURRENT)
            );
            AuditTrail auditTrail = injector.getInstance(AuditTrail.class);
            assertThat(auditTrail, instanceOf(AuditTrailService.class));
            AuditTrailService service = (AuditTrailService) auditTrail;
            assertThat(service.auditTrails, notNullValue());
            assertThat(service.auditTrails.length, is(1));
            assertThat(service.auditTrails[0], instanceOf(LoggingAuditTrail.class));
        } finally {
            pool.shutdown();
        }
    }

    public void testUnknownOutput() throws Exception {
        Settings settings = Settings.builder()
                .put("shield.audit.enabled", true)
                .put("shield.audit.outputs" , "foo")
                .put("client.type", "node")
                .build();
        SettingsModule settingsModule = new SettingsModule(settings, new SettingsFilter(settings));
        settingsModule.registerSetting(Setting.boolSetting("shield.audit.enabled", true, false, Setting.Scope.CLUSTER));
        settingsModule.registerSetting(Setting.simpleString("shield.audit.outputs", false, Setting.Scope.CLUSTER));
        try {
            Guice.createInjector(settingsModule, new AuditTrailModule(settings));
            fail("Expect initialization to fail when an unknown audit trail output is configured");
        } catch (Throwable t) {
            // expected
        }
    }
}
