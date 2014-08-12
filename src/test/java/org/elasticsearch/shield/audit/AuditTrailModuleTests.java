/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.shield.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class AuditTrailModuleTests extends ElasticsearchTestCase {

    @Test
    public void testEnabled() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.audit.enabled", false)
                .build();
        Injector injector = Guice.createInjector(new SettingsModule(settings), new AuditTrailModule(settings));
        AuditTrail auditTrail = injector.getInstance(AuditTrail.class);
        assertThat(auditTrail, is(AuditTrail.NOOP));
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        Settings settings = ImmutableSettings.EMPTY;
        Injector injector = Guice.createInjector(new SettingsModule(settings), new AuditTrailModule(settings));
        AuditTrail auditTrail = injector.getInstance(AuditTrail.class);
        assertThat(auditTrail, is(AuditTrail.NOOP));
    }

    @Test
    public void testLogfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.audit.enabled", true)
                .build();
        Injector injector = Guice.createInjector(new SettingsModule(settings), new AuditTrailModule(settings));
        AuditTrail auditTrail = injector.getInstance(AuditTrail.class);
        assertThat(auditTrail, instanceOf(AuditTrailService.class));
        AuditTrailService service = (AuditTrailService) auditTrail;
        assertThat(service.auditTrails, notNullValue());
        assertThat(service.auditTrails.length, is(1));
        assertThat(service.auditTrails[0], instanceOf(LoggingAuditTrail.class));
    }

}
