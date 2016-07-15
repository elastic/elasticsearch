/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;

public class AuditTrailModuleTests extends ModuleTestCase {

    public void testEnabled() throws Exception {
        Settings settings = Settings.builder().put(AuditTrailModule.ENABLED_SETTING.getKey(), true).build();
        AuditTrailModule module = new AuditTrailModule(settings);
        assertBinding(module, AuditTrail.class, AuditTrailService.class);
        assertSetMultiBinding(module, AuditTrail.class, LoggingAuditTrail.class);
    }

    public void testDisabledByDefault() throws Exception {
        AuditTrailModule module = new AuditTrailModule(Settings.EMPTY);
        assertInstanceBinding(module, AuditTrail.class, x -> x == AuditTrail.NOOP);
    }

    public void testIndexAuditTrail() throws Exception {
        Settings settings = Settings.builder()
            .put(AuditTrailModule.ENABLED_SETTING.getKey(), true)
            .put(AuditTrailModule.OUTPUTS_SETTING.getKey(), "index").build();
        AuditTrailModule module = new AuditTrailModule(settings);
        assertSetMultiBinding(module, AuditTrail.class, IndexAuditTrail.class);
    }

    public void testIndexAndLoggingAuditTrail() throws Exception {
        Settings settings = Settings.builder()
            .put(AuditTrailModule.ENABLED_SETTING.getKey(), true)
            .put(AuditTrailModule.OUTPUTS_SETTING.getKey(), "index,logfile").build();
        AuditTrailModule module = new AuditTrailModule(settings);
        assertSetMultiBinding(module, AuditTrail.class, IndexAuditTrail.class, LoggingAuditTrail.class);
    }

    public void testUnknownOutput() throws Exception {
        Settings settings = Settings.builder()
                .put(AuditTrailModule.ENABLED_SETTING.getKey(), true)
                .put(AuditTrailModule.OUTPUTS_SETTING.getKey(), "foo").build();
        AuditTrailModule module = new AuditTrailModule(settings);
        assertBindingFailure(module, "unknown audit trail output [foo]");
    }
}
