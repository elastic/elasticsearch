/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import com.google.common.collect.Sets;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.logfile.LoggingAuditTrail;

import java.util.Set;

/**
 *
 */
public class AuditTrailModule extends AbstractModule {

    private final Settings settings;

    public AuditTrailModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        if (settings.getAsBoolean("shield.audit.enabled", false)) {
            bind(AuditTrail.class).toInstance(AuditTrail.NOOP);
            return;
        }
        String[] outputs = settings.getAsArray("shield.audit.outputs", new String[] { "logfile" });
        if (outputs.length == 0) {
            bind(AuditTrail.class).toInstance(AuditTrail.NOOP);
            return;
        }
        bind(AuditTrail.class).to(AuditTrailService.class);
        Multibinder<AuditTrail> binder = Multibinder.newSetBinder(binder(), AuditTrail.class);

        Set<String> uniqueOutputs = Sets.newHashSet(outputs);
        for (String output : uniqueOutputs) {
            switch (output) {
                case "logfile":
                    binder.addBinding().to(LoggingAuditTrail.class);
                    break;
            }
        }
    }
}
